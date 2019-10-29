
[![pipeline status](https://gitlab.com/mdr0id/lightwalletd/badges/master/pipeline.svg)](https://gitlab.com/mdr0id/lightwalletd/commits/master)
[![coverage report](https://gitlab.com/mdr0id/lightwalletd/badges/master/coverage.svg)](https://gitlab.com/mdr0id/lightwalletd/commits/master)

# Overview

[lightwalletd](https://github.com/zcash-hackworks/lightwalletd) is a backend service that provides a bandwidth-efficient interface to the Zcash blockchain. Currently, lightwalletd supports the Sapling protocol version as its primary concern. The intended purpose of lightwalletd is to support the development of mobile-friendly shielded light wallets.

lightwalletd consists of three loosely coupled components: an "ingester", a "frontend", and an arbitrary storage layer (such as a SQL database) that connects the two. The ingester receives raw block data, parses out the transactions and block metadata, then stores them in a format convenient for the frontend to serve to clients. Thus, these components can operate and scale independently of each other and are connected only by a shared storage convention.

Lightwalletd has not yet undergone audits or been subject to rigorous testing. It lacks some affordances necessary for production-level reliability. We do not recommend using it to handle customer funds at this time (October 2019).

To view status of [CI pipeline](https://gitlab.com/mdr0id/lightwalletd/pipelines)

To view detailed [Codecov](https://codecov.io/gh/zcash-hackworks/lightwalletd) report

# Local/Developer Usage

First, ensure [Go >= 1.11](https://golang.org/dl/#stable) is installed. Once your go environment is setup correctly, you can build/run the below components.

To build ingest and server, run `make`.

This will build the ingest and server binaries, where you can use the below commands to configure how they run.

## To run INGESTER

Assuming you used `make` to build INGESTER

```
./ingest --conf-file /home/zcash/.zcash/zcash.conf --db-path /db/sql.db --log-file /logs/ingest.log
```

## To run SERVER

Assuming you used `make` to build SERVER:

```
./server --very-insecure=true --conf-file /home/zcash/.zcash/zcash.conf --db-path /db/sql.db --log-file /logs/server.log --bind-addr 127.0.0.1:18232
```

# Production Usage

Ensure [Go >= 1.11](https://golang.org/dl/#stable) is installed.

**x509 Certificates**
You will need to supply an x509 certificate that connecting clients will have good reason to trust (hint: do not use a self-signed one, our SDK will reject those unless you distribute them to the client out-of-band). We suggest that you be sure to buy a reputable one from a supplier that uses a modern hashing algorithm (NOT md5 or sha1) and that uses Certificate Transparency (OID 1.3.6.1.4.1.11129.2.4.2 will be present in the certificate).

To check a given certificate's (cert.pem) hashing algorithm:
```
openssl x509 -text -in certificate.crt | grep "Signature Algorithm"
```

To check if a given certificate (cert.pem) contains a Certificate Transparency OID:
```
echo "1.3.6.1.4.1.11129.2.4.2 certTransparency Certificate Transparency" > oid.txt
openssl asn1parse -in cert.pem -oid ./oid.txt | grep 'Certificate Transparency'
```

To use Let's Encrypt to generate a free certificate for your frontend, one method is to:
1) Install certbot
2) Open port 80 to your host
3) Point some forward dns to that host (some.forward.dns.com)
4) Run
```
certbot certonly --standalone --preferred-challenges http -d some.forward.dns.com
```
5) Pass the resulting certificate and key to frontend using the -tls-cert and -tls-key options.

## To run production INGESTER

Example using ingest binary built from Makefile:

```
./ingest --conf-file /home/zcash/.zcash/zcash.conf --db-path /db/sql.db --log-file /logs/ingest.log
```

## To run production SERVER

Example using server binary built from Makefile:

```
./server --tls-cert cert.pem --tls-key key.pem --conf-file /home/zcash/.zcash/zcash.conf --db-path /db/sql.db --log-file /logs/server.log --bind-addr 127.0.0.1:18232
```