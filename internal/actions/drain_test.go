package actions

// Action-layer coverage for the codec's streamed-Binary drain budget:
// a reply whose Binary claims an absurd length is a
// hostile or corrupt frame, and the turn must fail promptly with the connection
// poisoned rather than pin the worker on an unbounded discard loop.

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/sproto"
)

// TestDoRejectsAbsurdBinaryClaim drives a full do turn against a server that
// answers with a well-formed reply map whose "data" Binary declares 2^62 bytes
// and then trickles payload bytes forever. Before the drain budget the decoder
// read towards that claimed end: the turn never returned, so the op goroutine
// never finished, drainBatch's wg.Wait never completed and the whole sync cycle
// froze with no observable symptom. The turn must now return an error and mark
// the Client broken so the pool re-dials it.
func TestDoRejectsAbsurdBinaryClaim(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		// Hand-built because the encoder cannot produce a length that lies: a map
		// whose "data" value is a Binary claiming 1<<62 payload bytes.
		head := []byte{0x42, 0x10, 0x00, 0x04, 'd', 'a', 't', 'a', 0x30}
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], 1<<62)
		head = append(head, lenBuf[:]...)
		if _, err := server.Write(head); err != nil {
			return
		}
		// A live but never-finishing payload: the writes block until read and fail
		// once the client end closes, so a bounded decoder never asks for them.
		for {
			if _, err := server.Write([]byte{0x5A}); err != nil {
				return
			}
		}
	})
	c := newClient(conn, "session-token")

	errc := make(chan error, 1)
	go func() {
		_, err := c.do(testContext(t), "list", sproto.NewMap())
		errc <- err
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, sproto.ErrDrainTooLarge) {
			t.Fatalf("do error = %v, want sproto.ErrDrainTooLarge", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("do did not return: the reply drain is still unbounded")
	}
	if !c.Broken() {
		t.Error("Broken() = false; an aborted mid-frame drain leaves the connection misaligned")
	}
	if _, err := c.do(testContext(t), "list", sproto.NewMap()); !errors.Is(err, ErrConnBroken) {
		t.Errorf("second turn error = %v, want ErrConnBroken (the connection must not be reused)", err)
	}
}
