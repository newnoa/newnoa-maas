# NEWNOA Go Core

Go Core is the local runtime used by NEWNOA clients. It provides the local request boundary, browser surface, model process management and transport adapters.

## Build

This directory is a standalone Go module. Build it with Go 1.26 or later.

```sh
go build ./...
go test ./...
```

Runtime control and gateway addresses are supplied through environment variables. The public source contains no deployment credentials or private infrastructure configuration.
