## Overview
Server and client for a Dropbox-like data block storage and retrieval service run from the command line. Began life as an assignment for my Networked Systems class.

## How I built it
gRPC is the backbone for the communication protocol, including upload and download of files. Server and client are implemented in Java.

## What I learned
How to work with gRPC. Two-phase commit.

## What's next for Dropblox
Storage is currently not persistent - it is all stored in-memory, and thus data will be lost if server shuts down. Change this.

## Usage

### To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

### To build the code:

$ mvn package

### To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

### To run the client

$ target/surfstore/bin/runClient
