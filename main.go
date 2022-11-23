package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/textproto"
	"os"
	"sync/atomic"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

/*
Redis requests are arrays of strings. Arrays start with "*<n>" where <n> is the number of strings in the request.

Simple strings are preceeded by "+" followed by characters up to CRLF.

Bulk strings are preceeded by "$<m>" where <m> is the number of characters in the string. Followed by CRLF and then the
string followed again by CRLF (the string may include the CRLF sequence. The <m> count should be used to delimit bulk
strings)

Respose are bulk strings (integers start with ':', Errors start with '-')

Example of a GET command:

	*2
	$3
	GET
	$57
	csc[63472aad9a791211b792b0a9]nflw.app.93.47.3.21:50332111

And the response:

	$100
	{"1":"93.47.3.21:50332111","2":50332111,"-3":"nflw","3":"93.47.3.21","4":"alpes","-5":1666879784574}

Example of a SET command:

	*4
	$5
	SETEX
	$57
	csc[63472aad9a791211b792b0a9]nflw.app.93.47.3.21:50332111
	$4
	7754
	$100
	{"1":"93.47.3.21:50332111","2":50332111,"-3":"nflw","3":"93.47.3.21","4":"alpes","-5":1666879972576}

And the response:

	+OK

*/

var streamCount int32

// httpStreamFactory implements tcpassembly.StreamFactory
type httpStreamFactory struct{}

// httpStream will handle the actual decoding of http requests.
type httpStream struct {
	net, transport gopacket.Flow
	r              tcpreader.ReaderStream
	i              int32
}

func (h *httpStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	hstream := &httpStream{
		net:       net,
		transport: transport,
		r:         tcpreader.NewReaderStream(),
		i:         atomic.AddInt32(&streamCount, 1),
	}
	go hstream.run() // Important... we must guarantee that data from the reader stream is read.

	// ReaderStream implements tcpassembly.Stream, so we can return a pointer to it.
	return &hstream.r
}

func (h *httpStream) run() {
	buf := bufio.NewReader(&h.r)
	tp := textproto.NewReader(buf)
	for {
		line, err := tp.ReadLine()
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			return
		} else if err != nil {
			log.Println("Error reading stream", err)
		} else {
			fmt.Printf("%10d: %s:%s -> %s:%s: %s\n", h.i, h.net.Src(), h.transport.Src(), h.net.Dst(), h.transport.Dst(), line)
		}
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("expected pcap filename argument")
	}

	filename := os.Args[1]

	f, err := os.Open(filename)
	if err != nil {
		log.Fatal("failed to open file:", err)
	}
	defer f.Close()

	pcapReader, err := pcapgo.NewReader(f)

	var count int
	var size int
	var originalSize int

	// Set up assembly
	streamFactory := &httpStreamFactory{}
	streamPool := tcpassembly.NewStreamPool(streamFactory)
	assembler := tcpassembly.NewAssembler(streamPool)

	for {
		data, ci, err := pcapReader.ReadPacketData()
		if err != nil && err != io.EOF {
			log.Fatal("reading packet", err)
		} else if err == io.EOF {
			break
		}
		count++
		size += len(data)
		originalSize += ci.Length

		packet := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.Default)
		if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
			// Get actual TCP data from this layer
			tcp, _ := tcpLayer.(*layers.TCP)
			assembler.AssembleWithTimestamp(packet.NetworkLayer().NetworkFlow(), tcp, packet.Metadata().Timestamp)
		}

	}
	assembler.FlushAll()
	fmt.Printf("read %d packets, size %d bytes, original size %d bytes\n", count, size, originalSize)
}
