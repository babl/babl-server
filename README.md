# Babl Server

A babl client can connect directly to a bable module throw the babl server, that is a simple GRPC Server using protobuf as message protocol. In order to be able to run many replicas of the same babl module, it uses kafka as message queue, being the supervisor module the proxy for the sync requests.
