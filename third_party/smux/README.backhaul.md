# smux (vendored, patched)

Copy of github.com/xtaci/smux v1.5.27 (MIT, see LICENSE) without its tests and
pictures, with one change: `stream.go` sends window updates every
`MaxStreamBuffer/8` consumed bytes instead of every half window (`windowUpdateDivisor`).
See the comment there for why. The wire format is unchanged. To go back to the
upstream module, delete this directory and the `replace` line in `go.mod`.
