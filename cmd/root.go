package cmd

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/rpcclient"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	"github.com/zcash/lightwalletd/common"
	"github.com/zcash/lightwalletd/common/logging"
	"github.com/zcash/lightwalletd/frontend"
	"github.com/zcash/lightwalletd/walletrpc"
)

var cfgFile string
var logger = logrus.New()

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "lightwalletd",
	Short: "Lightwalletd is a backend service to the Zcash blockchain",
	Long: `Lightwalletd is a backend service that provides a 
         bandwidth-efficient interface to the Zcash blockchain`,
	Run: func(cmd *cobra.Command, args []string) {
		opts := &common.Options{
			GRPCBindAddr:        viper.GetString("grpc-bind-addr"),
			GRPCLogging:         viper.GetBool("grpc-logging-insecure"),
			HTTPBindAddr:        viper.GetString("http-bind-addr"),
			TLSCertPath:         viper.GetString("tls-cert"),
			TLSKeyPath:          viper.GetString("tls-key"),
			LogLevel:            viper.GetUint64("log-level"),
			LogFile:             viper.GetString("log-file"),
			ZcashConfPath:       viper.GetString("zcash-conf-path"),
			RPCUser:             viper.GetString("rpcuser"),
			RPCPassword:         viper.GetString("rpcpassword"),
			RPCHost:             viper.GetString("rpchost"),
			RPCPort:             viper.GetString("rpcport"),
			NoTLSVeryInsecure:   viper.GetBool("no-tls-very-insecure"),
			GenCertVeryInsecure: viper.GetBool("gen-cert-very-insecure"),
			DataDir:             viper.GetString("data-dir"),
			Redownload:          viper.GetBool("redownload"),
			PingEnable:          viper.GetBool("ping-very-insecure"),
			Darkside:            viper.GetBool("darkside-very-insecure"),
			DarksideTimeout:     viper.GetUint64("darkside-timeout"),
		}

		common.Log.Debugf("Options: %#v\n", opts)

		filesThatShouldExist := []string{
			opts.LogFile,
		}
		if !fileExists(opts.LogFile) {
			os.OpenFile(opts.LogFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		}
		if !opts.Darkside && (opts.RPCUser == "" || opts.RPCPassword == "" || opts.RPCHost == "" || opts.RPCPort == "") {
			filesThatShouldExist = append(filesThatShouldExist, opts.ZcashConfPath)
		}
		if !opts.NoTLSVeryInsecure && !opts.GenCertVeryInsecure {
			filesThatShouldExist = append(filesThatShouldExist,
				opts.TLSCertPath, opts.TLSKeyPath)
		}

		for _, filename := range filesThatShouldExist {
			if !fileExists(filename) {
				os.Stderr.WriteString(fmt.Sprintf("\n  ** File does not exist: %s\n\n", filename))
				common.Log.Fatal("required file ", filename, " does not exist")
			}
		}

		// Start server and block, or exit
		if err := startServer(opts); err != nil {
			common.Log.WithFields(logrus.Fields{
				"error": err,
			}).Fatal("couldn't create server")
		}
	},
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func startServer(opts *common.Options) error {
	if opts.LogFile != "" {
		// instead write parsable logs for logstash/splunk/etc
		output, err := os.OpenFile(opts.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			common.Log.WithFields(logrus.Fields{
				"error": err,
				"path":  opts.LogFile,
			}).Fatal("couldn't open log file")
		}
		defer output.Close()
		logger.SetOutput(output)
		logger.SetFormatter(&logrus.JSONFormatter{})
	}

	logger.SetLevel(logrus.Level(opts.LogLevel))

	common.Log.WithFields(logrus.Fields{
		"gitCommit": common.GitCommit,
		"buildDate": common.BuildDate,
		"buildUser": common.BuildUser,
	}).Infof("Starting gRPC server version %s on %s", common.Version, opts.GRPCBindAddr)

	logging.LogToStderr = opts.GRPCLogging

	// gRPC initialization
	var server *grpc.Server

	if opts.NoTLSVeryInsecure {
		common.Log.Warningln("Starting insecure no-TLS (plaintext) server")
		fmt.Println("Starting insecure server")
		server = grpc.NewServer(
			grpc.StreamInterceptor(
				grpc_middleware.ChainStreamServer(
					grpc_prometheus.StreamServerInterceptor),
			),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				logging.LogInterceptor,
				grpc_prometheus.UnaryServerInterceptor),
			))
	} else {
		var transportCreds credentials.TransportCredentials
		if opts.GenCertVeryInsecure {
			common.Log.Warning("Certificate and key not provided, generating self signed values")
			fmt.Println("Starting insecure self-certificate server")
			tlsCert := common.GenerateCerts()
			transportCreds = credentials.NewServerTLSFromCert(tlsCert)
		} else {
			var err error
			transportCreds, err = credentials.NewServerTLSFromFile(opts.TLSCertPath, opts.TLSKeyPath)
			if err != nil {
				common.Log.WithFields(logrus.Fields{
					"cert_file": opts.TLSCertPath,
					"key_path":  opts.TLSKeyPath,
					"error":     err,
				}).Fatal("couldn't load TLS credentials")
			}
		}
		server = grpc.NewServer(
			grpc.Creds(transportCreds),
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
				grpc_prometheus.StreamServerInterceptor),
			),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				logging.LogInterceptor,
				grpc_prometheus.UnaryServerInterceptor),
			))
	}
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpc_prometheus.Register(server)
	go startHTTPServer(opts)

	// Enable reflection for debugging
	if opts.LogLevel >= uint64(logrus.WarnLevel) {
		reflection.Register(server)
	}

	// Initialize Zcash RPC client. Right now (Jan 2018) this is only for
	// sending transactions, but in the future it could back a different type
	// of block streamer.

	var saplingHeight int
	var chainName string
	var rpcClient *rpcclient.Client
	var err error
	if opts.Darkside {
		chainName = "darkside"
	} else {
		if opts.RPCUser != "" && opts.RPCPassword != "" && opts.RPCHost != "" && opts.RPCPort != "" {
			rpcClient, err = frontend.NewZRPCFromFlags(opts)
		} else {
			rpcClient, err = frontend.NewZRPCFromConf(opts.ZcashConfPath)
		}
		if err != nil {
			common.Log.WithFields(logrus.Fields{
				"error": err,
			}).Fatal("setting up RPC connection to zcashd")
		}
		// Indirect function for test mocking (so unit tests can talk to stub functions).
		common.RawRequest = rpcClient.RawRequest

		// Ensure that we can communicate with zcashd
		common.FirstRPC()

		getLightdInfo, err := common.GetLightdInfo()
		if err != nil {
			common.Log.WithFields(logrus.Fields{
				"error": err,
			}).Fatal("getting initial information from zcashd")
		}
		common.Log.Info("Got sapling height ", getLightdInfo.SaplingActivationHeight,
			" block height ", getLightdInfo.BlockHeight,
			" chain ", getLightdInfo.ChainName,
			" branchID ", getLightdInfo.ConsensusBranchId)
		saplingHeight = int(getLightdInfo.SaplingActivationHeight)
		chainName = getLightdInfo.ChainName
	}

	dbPath := filepath.Join(opts.DataDir, "db")
	if opts.Darkside {
		os.RemoveAll(filepath.Join(dbPath, chainName))
	}

	// Temporary, because PR 320 put the db files in the wrong place
	// (one level too high, directly in "db/" instead of "db/chainname"),
	// so delete them if they're present. This can be removed sometime.
	os.Remove(filepath.Join(dbPath, "blocks"))
	os.Remove(filepath.Join(dbPath, "blocks-corrupted"))
	os.Remove(filepath.Join(dbPath, "lengths"))
	os.Remove(filepath.Join(dbPath, "lengths-corrupted"))

	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("\n  ** Can't create data directory: %s\n\n", opts.DataDir))
		os.Exit(1)
	}
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("\n  ** Can't create db directory: %s\n\n", dbPath))
		os.Exit(1)
	}
	cache := common.NewBlockCache(dbPath, chainName, saplingHeight, opts.Redownload)
	if !opts.Darkside {
		go common.BlockIngestor(cache, 0 /*loop forever*/)
	} else {
		// Darkside wants to control starting the block ingestor.
		common.DarksideInit(cache, int(opts.DarksideTimeout))
	}

	// Compact transaction service initialization
	{
		service, err := frontend.NewLwdStreamer(cache, chainName, opts.PingEnable)
		if err != nil {
			common.Log.WithFields(logrus.Fields{
				"error": err,
			}).Fatal("couldn't create backend")
		}
		walletrpc.RegisterCompactTxStreamerServer(server, service)
	}
	if opts.Darkside {
		service, err := frontend.NewDarksideStreamer(cache)
		if err != nil {
			common.Log.WithFields(logrus.Fields{
				"error": err,
			}).Fatal("couldn't create backend")
		}
		walletrpc.RegisterDarksideStreamerServer(server, service)
	}

	// Initialize mempool monitor
	exitMempool := make(chan bool)
	common.StartMempoolMonitor(cache, exitMempool)

	// Start listening
	listener, err := net.Listen("tcp", opts.GRPCBindAddr)
	if err != nil {
		common.Log.WithFields(logrus.Fields{
			"bind_addr": opts.GRPCBindAddr,
			"error":     err,
		}).Fatal("couldn't create listener")
	}

	// Signal handler for graceful stops
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-signals
		cache.Sync()
		exitMempool <- true
		common.Log.WithFields(logrus.Fields{
			"signal": s.String(),
		}).Info("caught signal, stopping gRPC server")
		os.Exit(1)
	}()

	err = server.Serve(listener)
	if err != nil {
		common.Log.WithFields(logrus.Fields{
			"error": err,
		}).Fatal("gRPC server exited")
	}
	return nil
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(versionCmd)
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is current directory, lightwalletd.yaml)")
	rootCmd.Flags().String("http-bind-addr", "127.0.0.1:9068", "the address to listen for http on")
	rootCmd.Flags().String("grpc-bind-addr", "127.0.0.1:9067", "the address to listen for grpc on")
	rootCmd.Flags().Bool("grpc-logging-insecure", false, "enable grpc logging to stderr")
	rootCmd.Flags().String("tls-cert", "./cert.pem", "the path to a TLS certificate")
	rootCmd.Flags().String("tls-key", "./cert.key", "the path to a TLS key file")
	rootCmd.Flags().Int("log-level", int(logrus.InfoLevel), "log level (logrus 1-7)")
	rootCmd.Flags().String("log-file", "./server.log", "log file to write to")
	rootCmd.Flags().String("zcash-conf-path", "./zcash.conf", "conf file to pull RPC creds from")
	rootCmd.Flags().String("rpcuser", "", "RPC user name")
	rootCmd.Flags().String("rpcpassword", "", "RPC password")
	rootCmd.Flags().String("rpchost", "", "RPC host")
	rootCmd.Flags().String("rpcport", "", "RPC host port")
	rootCmd.Flags().Bool("no-tls-very-insecure", false, "run without the required TLS certificate, only for debugging, DO NOT use in production")
	rootCmd.Flags().Bool("gen-cert-very-insecure", false, "run with self-signed TLS certificate, only for debugging, DO NOT use in production")
	rootCmd.Flags().Bool("redownload", false, "re-fetch all blocks from zcashd; reinitialize local cache files")
	rootCmd.Flags().String("data-dir", "/var/lib/lightwalletd", "data directory (such as db)")
	rootCmd.Flags().Bool("ping-very-insecure", false, "allow Ping GRPC for testing")
	rootCmd.Flags().Bool("darkside-very-insecure", false, "run with GRPC-controllable mock zcashd for integration testing (shuts down after 30 minutes)")
	rootCmd.Flags().Int("darkside-timeout", 30, "override 30 minute default darkside timeout")

	viper.BindPFlag("grpc-bind-addr", rootCmd.Flags().Lookup("grpc-bind-addr"))
	viper.SetDefault("grpc-bind-addr", "127.0.0.1:9067")
	viper.BindPFlag("grpc-logging-insecure", rootCmd.Flags().Lookup("grpc-logging-insecure"))
	viper.SetDefault("grpc-logging-insecure", false)
	viper.BindPFlag("http-bind-addr", rootCmd.Flags().Lookup("http-bind-addr"))
	viper.SetDefault("http-bind-addr", "127.0.0.1:9068")
	viper.BindPFlag("tls-cert", rootCmd.Flags().Lookup("tls-cert"))
	viper.SetDefault("tls-cert", "./cert.pem")
	viper.BindPFlag("tls-key", rootCmd.Flags().Lookup("tls-key"))
	viper.SetDefault("tls-key", "./cert.key")
	viper.BindPFlag("log-level", rootCmd.Flags().Lookup("log-level"))
	viper.SetDefault("log-level", int(logrus.InfoLevel))
	viper.BindPFlag("log-file", rootCmd.Flags().Lookup("log-file"))
	viper.SetDefault("log-file", "./server.log")
	viper.BindPFlag("zcash-conf-path", rootCmd.Flags().Lookup("zcash-conf-path"))
	viper.SetDefault("zcash-conf-path", "./zcash.conf")
	viper.BindPFlag("rpcuser", rootCmd.Flags().Lookup("rpcuser"))
	viper.BindPFlag("rpcpassword", rootCmd.Flags().Lookup("rpcpassword"))
	viper.BindPFlag("rpchost", rootCmd.Flags().Lookup("rpchost"))
	viper.BindPFlag("rpcport", rootCmd.Flags().Lookup("rpcport"))
	viper.BindPFlag("no-tls-very-insecure", rootCmd.Flags().Lookup("no-tls-very-insecure"))
	viper.SetDefault("no-tls-very-insecure", false)
	viper.BindPFlag("gen-cert-very-insecure", rootCmd.Flags().Lookup("gen-cert-very-insecure"))
	viper.SetDefault("gen-cert-very-insecure", false)
	viper.BindPFlag("redownload", rootCmd.Flags().Lookup("redownload"))
	viper.SetDefault("redownload", false)
	viper.BindPFlag("data-dir", rootCmd.Flags().Lookup("data-dir"))
	viper.SetDefault("data-dir", "/var/lib/lightwalletd")
	viper.BindPFlag("ping-very-insecure", rootCmd.Flags().Lookup("ping-very-insecure"))
	viper.SetDefault("ping-very-insecure", false)
	viper.BindPFlag("darkside-very-insecure", rootCmd.Flags().Lookup("darkside-very-insecure"))
	viper.SetDefault("darkside-very-insecure", false)
	viper.BindPFlag("darkside-timeout", rootCmd.Flags().Lookup("darkside-timeout"))
	viper.SetDefault("darkside-timeout", 30)

	logger.SetFormatter(&logrus.TextFormatter{
		//DisableColors:          true,
		FullTimestamp:          true,
		DisableLevelTruncation: true,
	})

	onexit := func() {
		fmt.Printf("Lightwalletd died with a Fatal error. Check logfile for details.\n")
	}

	common.Log = logger.WithFields(logrus.Fields{
		"app": "lightwalletd",
	})

	logrus.RegisterExitHandler(onexit)

	// Indirect function for test mocking (so unit tests can talk to stub functions)
	common.Sleep = time.Sleep
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Look in the current directory for a configuration file
		viper.AddConfigPath(".")
		// Viper auto appends extention to this config name
		// For example, lightwalletd.yml
		viper.SetConfigName("lightwalletd")
	}

	// Replace `-` in config options with `_` for ENV keys
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv() // read in environment variables that match
	// If a config file is found, read it in.
	var err error
	if err = viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

}

func startHTTPServer(opts *common.Options) {
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(opts.HTTPBindAddr, nil)
}
