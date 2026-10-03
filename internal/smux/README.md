# smux (vendored, patched)

Copy of github.com/xtaci/smux v1.5.27 (MIT, see LICENSE) without its tests and
pictures, living inside this module (not as a nested module, so `go install
.../backhaul@version` and module zips keep working), with one change: `stream.go`
sends window updates every `MaxStreamBuffer/8` consumed bytes instead of every half
window (`windowUpdateDivisor`). See the comment there for why. The wire format is
unchanged, so peers running upstream smux interoperate.

To go back to the upstream module: restore the `github.com/xtaci/smux` requirement in
go.mod, point the imports back and delete this directory.
