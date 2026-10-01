package dnsx

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
)

const authNonceLen = 16

var errAuth = errors.New("dnsx: peer failed authentication")

func authTag(token, role string, a, b []byte) []byte {
	m := hmac.New(sha256.New, []byte(token))
	m.Write([]byte(role))
	m.Write(a)
	m.Write(b)
	return m.Sum(nil)
}

// ClientAuth proves knowledge of token to the server and checks the server's
// proof in return (mutual challenge-response). The token itself never crosses the
// carrier, whose client->server bytes are only base32 in QNAMEs and readable by
// any resolver or passive observer. Set a deadline on conn first.
func ClientAuth(conn net.Conn, token string) error {
	cn := make([]byte, authNonceLen)
	if _, err := rand.Read(cn); err != nil {
		return err
	}
	if _, err := conn.Write(cn); err != nil {
		return fmt.Errorf("dnsx: auth hello: %w", err)
	}
	reply := make([]byte, authNonceLen+sha256.Size)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fmt.Errorf("dnsx: auth challenge: %w", err)
	}
	sn, tag := reply[:authNonceLen], reply[authNonceLen:]
	if !hmac.Equal(tag, authTag(token, "srv", cn, sn)) {
		return errAuth
	}
	if _, err := conn.Write(authTag(token, "cli", sn, cn)); err != nil {
		return fmt.Errorf("dnsx: auth response: %w", err)
	}
	return nil
}

// ServerAuth is the server half of ClientAuth.
func ServerAuth(conn net.Conn, token string) error {
	cn := make([]byte, authNonceLen)
	if _, err := io.ReadFull(conn, cn); err != nil {
		return fmt.Errorf("dnsx: auth hello: %w", err)
	}
	sn := make([]byte, authNonceLen)
	if _, err := rand.Read(sn); err != nil {
		return err
	}
	if _, err := conn.Write(append(sn, authTag(token, "srv", cn, sn)...)); err != nil {
		return fmt.Errorf("dnsx: auth challenge: %w", err)
	}
	tag := make([]byte, sha256.Size)
	if _, err := io.ReadFull(conn, tag); err != nil {
		return fmt.Errorf("dnsx: auth response: %w", err)
	}
	if !hmac.Equal(tag, authTag(token, "cli", sn, cn)) {
		return errAuth
	}
	return nil
}
