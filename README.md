## thrift-nodejs

This is a stripped-down version of the thrift nodejs library installable
directly as a package and containing a number of performance optimisations.

Its primary purpose is to support the Ably nodejs client library and therefore
it excludes support for:

- RPC;
- any transport other than TTransport;
- any protocol other than TBinaryProtocol.

Support for other transports/protocols may be added in the future.

There is an Ably fork of thrift, and this project tracks changes to that
project.