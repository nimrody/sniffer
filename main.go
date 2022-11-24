package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/textproto"
	"os"
	"strconv"
	"sync/atomic"
	"time"

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

Types of transactions:

1. GET commands

	*2
	$3
	GET
	$57
	csc[63472aad9a791211b792b0a9]nflw.app.93.47.3.21:50332111

And the response:

	$100
	{"1":"93.47.3.21:50332111","2":50332111,"-3":"nflw","3":"93.47.3.21","4":"alpes","-5":1666879784574}

2. SET commands

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

3. PING/PONG keepalives

	PING is answered with a PONG

4. Notifications

	Response only
	"pmessage", "*", "__keyevent@0__:set", "csc[63472aad9a791211b792b0a9]wsa.clonbrd.CA:DA:DC:23:8A:61"

*/

const (
	redisPort = 6379
)

var streamCount int32

// redisStreamFactory implements tcpassembly.StreamFactory
type redisStreamFactory struct{}

// redisStream will handle the actual decoding of redis requests.
type redisStream struct {
	net, transport gopacket.Flow
	reader         tcpreader.ReaderStream
	streamIndex    int32
	lastTimestamp  time.Time
	clientRequest  bool // true if this is a flow from the client to the server, false otherwise
}

// Reassembled implements tcpassembly.Stream
func (s *redisStream) Reassembled(segments []tcpassembly.Reassembly) {
	for i, segment := range segments {
		s.lastTimestamp = segment.Seen
		s.reader.Reassembled(segments[i : i+1])
	}
}

// ReassemblyComplete implements tcpassembly.Stream
func (s *redisStream) ReassemblyComplete() {
	s.reader.ReassemblyComplete()
}

func (*redisStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	dstPortRaw := transport.Dst().Raw()
	dstPort := uint16(dstPortRaw[0])<<8 | uint16(dstPortRaw[1])

	rstream := &redisStream{
		net:           net,
		transport:     transport,
		reader:        tcpreader.NewReaderStream(),
		streamIndex:   atomic.AddInt32(&streamCount, 1),
		lastTimestamp: time.Time{},
		clientRequest: dstPort == redisPort,
	}

	// Important... we must guarantee that data from the reader stream is read.
	if rstream.clientRequest {
		go rstream.handleRequests()
	} else {
		go rstream.handleResponses()
	}
	// ReaderStream implements tcpassembly.Stream, so we can return a pointer to it.
	return rstream
}

func redisReadString(tp *textproto.Reader) (string, error) {
	line, err := tp.ReadLine()
	if err == io.EOF {
		return "", err
	}
	if line[0] == '+' { // beginning of a simple string
		line = line[1:]
	} else if line[0] == '$' { // beginning of a bulk string
		n, _ := strconv.Atoi(line[1:])
		line, err = tp.ReadLine() // TODO: should read n characters and ignore CRLF
		if err == io.EOF {
			return "", err
		}
		if len(line) < n {
			log.Fatal(fmt.Sprintf("expected %d characters, got %d characters: %s", n, len(line), line))
		}
	}
	return line, nil
}

func (s *redisStream) handleRequests() {
	buf := bufio.NewReader(&s.reader)
	tp := textproto.NewReader(buf)
	for {
		line, err := tp.ReadLine()
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			return
		} else if err != nil {
			log.Println("Error reading stream", err)
		} else if line[0] == '*' { // beginning of an array (used for sending commnads)
			n, _ := strconv.Atoi(line[1:])
			// read n strings
			lines := make([]string, 0, n)
			for i := 0; i < n; i++ {
				line, err = redisReadString(tp)
				if err != nil {
					return
				}
				lines = append(lines, line)
			}
			switch lines[0] {
			case "GET":
				key := lines[1]
				fmt.Printf("%10d: %s: GET %s\n", s.streamIndex, s.lastTimestamp, key)
			case "SETEX":
				key := lines[1]
				ttl, _ := strconv.Atoi(lines[2])
				value := lines[3]
				fmt.Printf("%10d: %s: SETEX %s = %s, TTL: %d\n", s.streamIndex, s.lastTimestamp, key, value, ttl)
			case "PING":
				// do nothing
			default:
			}
		}

		// fmt.Printf("%10d: %s: %s:%s -> %s:%s: %s\n", s.streamIndex, s.lastTimestamp, s.net.Src(), s.transport.Src(), s.net.Dst(), s.transport.Dst(), line)
	}
}

/*
Responses are typically a single value (OK, PONG, get-response) but

	may also be arrays if this is a key event
*/
func (s *redisStream) handleResponses() {
	buf := bufio.NewReader(&s.reader)
	tp := textproto.NewReader(buf)
	for {
		line, err := tp.ReadLine()
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			return
		} else if err != nil {
			log.Println("Error reading stream", err)
		} else if line[0] == '*' { // beginning of an array (used for sending commnads)
			n, _ := strconv.Atoi(line[1:])
			// read n strings
			lines := make([]string, 0, n)
			for i := 0; i < n; i++ {
				line, err = redisReadString(tp)
				if err != nil {
					return
				}
				lines = append(lines, line)
			}
			switch lines[0] {
			case "GET":
				key := lines[1]
				fmt.Printf("%10d: %s: GET %s\n", s.streamIndex, s.lastTimestamp, key)
			case "SETEX":
				key := lines[1]
				ttl, _ := strconv.Atoi(lines[2])
				value := lines[3]
				fmt.Printf("%10d: %s: SETEX %s = %s, TTL: %d\n", s.streamIndex, s.lastTimestamp, key, value, ttl)
			case "PING":
				// do nothing
			default:
			}
		}

		// fmt.Printf("%10d: %s: %s:%s -> %s:%s: %s\n", s.streamIndex, s.lastTimestamp, s.net.Src(), s.transport.Src(), s.net.Dst(), s.transport.Dst(), line)
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
	streamFactory := &redisStreamFactory{}
	streamPool := tcpassembly.NewStreamPool(streamFactory)
	assembler := tcpassembly.NewAssembler(streamPool)

	for {
		data, captureInfo, err := pcapReader.ReadPacketData()
		if err != nil && err != io.EOF {
			log.Fatal("reading packet", err)
		} else if err == io.EOF {
			break
		}
		count++
		size += len(data)
		originalSize += captureInfo.Length

		packet := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.Default)
		if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
			// Get actual TCP data from this layer
			tcp, _ := tcpLayer.(*layers.TCP)
			assembler.AssembleWithTimestamp(packet.NetworkLayer().NetworkFlow(), tcp, captureInfo.Timestamp)
		}

	}
	assembler.FlushAll()
	fmt.Printf("read %d packets, size %d bytes, original size %d bytes\n", count, size, originalSize)
}
