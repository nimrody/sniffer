// Copyright 2012 Google, Inc. All rights reserved.
//
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

// Package tcpreader provides an implementation for tcpassembly.Stream which presents
// the caller with an io.Reader for easy processing.
//
// The assembly package handles packet data reordering, but its output is
// library-specific, thus not usable by the majority of external Go libraries.
// The io.Reader interface, on the other hand, is used throughout much of Go
// code as an easy mechanism for reading in data streams and decoding them.  For
// example, the net/http package provides the ReadRequest function, which can
// parse an HTTP request from a live data stream, just what we'd want when
// sniffing HTTP traffic.  Using ReaderStream, this is relatively easy to set
// up:
//
//	// Create our StreamFactory
//	type httpStreamFactory struct {}
//	func (f *httpStreamFactory) New(a, b gopacket.Flow) tcpassembly.Stream {
//		r := tcpreader.NewReaderStream()
//		go printRequests(&r, a, b)
//		return &r
//	}
//	func printRequests(r io.Reader, a, b gopacket.Flow) {
//		// Convert to bufio, since that's what ReadRequest wants.
//		buf := bufio.NewReader(r)
//		for {
//			if req, err := http.ReadRequest(buf); err == io.EOF {
//				return
//			} else if err != nil {
//				log.Println("Error parsing HTTP requests:", err)
//			} else {
//				fmt.Println(a, b)
//				fmt.Println("HTTP REQUEST:", req)
//				fmt.Println("Body contains", tcpreader.DiscardBytesToEOF(req.Body), "bytes")
//			}
//		}
//	}
//
// Using just this code, we're able to reference a powerful, built-in library
// for HTTP request parsing to do all the dirty-work of parsing requests from
// the wire in real-time.  Pass this stream factory to an tcpassembly.StreamPool,
// start up an tcpassembly.Assembler, and you're good to go!
package tcpreader

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/google/gopacket/tcpassembly"
)

// ReaderStream implements both tcpassembly.Stream and io.Reader.  You can use it
// as a building block to make simple, easy stream handlers.
//
// IMPORTANT:  If you use a ReaderStream, you MUST read ALL BYTES from it,
// quickly.  Not reading available bytes will block TCP stream reassembly.  It's
// a common pattern to do this by starting a goroutine in the factory's New
// method:
//
//	type myStreamHandler struct {
//		r ReaderStream
//	}
//	func (m *myStreamHandler) run() {
//		// Do something here that reads all of the ReaderStream, or your assembly
//		// will block.
//		fmt.Println(tcpreader.DiscardBytesToEOF(&m.r))
//	}
//	func (f *myStreamFactory) New(a, b gopacket.Flow) tcpassembly.Stream {
//		s := &myStreamHandler{}
//		go s.run()
//		// Return the ReaderStream as the stream that assembly should populate.
//		return &s.r
//	}
type ReaderStream struct {
	ReaderStreamOptions
	reassembled      chan []tcpassembly.Reassembly
	current          []tcpassembly.Reassembly
	currentByteIndex int
	initiated        bool
	label            string
}

// ReaderStreamOptions provides user-resettable options for a ReaderStream.
type ReaderStreamOptions struct {
	// LossErrors determines whether this stream will return
	// ReaderStreamDataLoss errors from its Read function whenever it
	// determines data has been lost.
	LossErrors bool
}

// NewReaderStream returns a new ReaderStream object.
func NewReaderStream(label string) *ReaderStream {
	return &ReaderStream{
		reassembled: make(chan []tcpassembly.Reassembly, 1000),
		initiated:   true,
		label:       label,
	}
}

// Reassembled implements tcpassembly.Stream's Reassembled function.
func (r *ReaderStream) Reassembled(reassembly []tcpassembly.Reassembly) {
	if !r.initiated {
		panic("ReaderStream not created via NewReaderStream")
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d:[", len(reassembly)))
	for _, segment := range reassembly {
		sb.WriteString(fmt.Sprintf("%q,", string(segment.Bytes)))
	}
	sb.WriteByte(']')
	log.Printf("%s: Reassembled: %v\n", r.label, sb.String())

	reassemblyClone := make([]tcpassembly.Reassembly, len(reassembly))
	for i := 0; i < len(reassembly); i++ {
		r := tcpassembly.Reassembly{Bytes: make([]byte, len(reassembly[i].Bytes)), Seen: reassembly[i].Seen}
		copy(r.Bytes, reassembly[i].Bytes)
		reassemblyClone[i] = r
	}

	select {
	case r.reassembled <- reassemblyClone:
	default:
		panic("blocked on sending to channel")
	}
}

// ReassemblyComplete implements tcpassembly.Stream's ReassemblyComplete function.
// Called when the TCP stream is closed
func (r *ReaderStream) ReassemblyComplete() {
	close(r.reassembled)
}

// Read implements io.Reader's Read function.
// Given a byte slice, it will either copy a non-zero number of bytes into
// that slice and return the number of bytes and a nil error, or it will
// leave slice p as is and return 0, io.EOF.
func (r *ReaderStream) read() (byte, time.Time, error) {
	if !r.initiated {
		panic("ReaderStream not created via NewReaderStream")
	}

	// we have a segment to read from
	if len(r.current) > 0 {
		// not yet done with this segment
		if r.currentByteIndex < len(r.current[0].Bytes) {
			b := r.current[0].Bytes[r.currentByteIndex]
			r.currentByteIndex++
			return b, r.current[0].Seen, nil
		}

		// otherwise, done with the current segment. Prepare for the next
		r.currentByteIndex = 0
		r.current = r.current[1:]
		return r.read()
	}

	// no segments - fetch from channel
	var ok bool
	r.current, ok = <-r.reassembled
	r.currentByteIndex = 0

	if !ok {
		return 0, time.Time{}, io.EOF
	}

	return r.read()
}

// Close implements io.Closer's Close function, making ReaderStream a
// io.ReadCloser.  It discards all remaining bytes in the reassembly in a
// manner that's safe for the assembler (IE: it doesn't block).
func (r *ReaderStream) Close() error {
	panic("closed called")
	// r.current = nil
	// for {
	// 	if _, ok := <-r.reassembled; !ok {
	// 		return nil
	// 	}
	// }
}

func (r *ReaderStream) ReadLine(caller string) (string, time.Time, error) {
	var sb strings.Builder
	for {
		b, timestamp, error := r.read()
		if error != nil {
			fmt.Printf("ReadString %s returned ERROR %q %q\n", caller, error, io.EOF)
			return sb.String(), timestamp, error
		}
		sb.WriteByte(b) // will return the delimiter too
		if b == '\n' {
			line := strings.TrimSuffix(sb.String(), "\r\n")

			log.Printf("%p ReadString %v returned %q\n", r, caller, line)
			if len(line) == 0 {
				log.Fatalf("empty line %s\n", sb.String())
			}
			return line, timestamp, nil
		}
	}
}

func (r *ReaderStream) Fill() {
	// panic("todo")
	// nop
}
