# NEWNOA Go Server

Go Server handles server-side connections and request forwarding for NEWNOA.

## Build

From this directory, build the Go Server modules with Go.

```sh
for manifest in ./*/go.mod; do
  go -C "${manifest%/go.mod}" build ./... || exit 1
done
```

Supply your own service addresses, certificates and credentials at deployment time.
