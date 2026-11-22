# NEWNOA Go Server

Go Server is the server-side transport and forwarding runtime used by NEWNOA. It contains the gateway, owner and network implementations in one public component.

## Build

Build each Go Server module independently.

```sh
(cd gateway && go build ./...)
(cd owner && go build ./...)
(cd network && go build ./...)
```

Runtime addresses, certificates, upstream policy and credentials are supplied through environment variables. The public source contains no deployment credentials or private infrastructure configuration.
