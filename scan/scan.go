package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/google/gopacket/tcpassembly"
	"github.com/nimrody/my-sinffer/tcpreader"
)

/*
Redis requests are arrays of strings. Arrays start with "*<n>" where <n> is the number of strings in the request.

Simple strings are preceeded by "+" followed by characters up to CRLF.

Bulk strings are preceeded by "$<m>" where <m> is the number of characters in the string. Followed by CRLF and then the
string followed again by CRLF (the string may include the CRLF sequence. The <m> count should be used to delimit bulk
strings)

Respose are bulk strings (integers start with ':', Errors start with '-')

Integers are encoded as ':<number>'

Types of transactions:

1. GET commands
	["GET", <key-string>] -> <value-string>

2. SETEX commands (SET + EXPIRE combination)
	["SETEX", <key-string>, <value-string> <number-string>] -> "OK"

3. SET command
 	["SET", <key-string>, <value-string>] -> "OK"

4. SETNX command (Set if not exists)
 	["SET", <key-string>, <value-string>] -> 0 or 1  (1 if set, 0 if not)

5. EXPIRE
	["EXPIRE", <key-string>, <number-string>] -> 0 or 1  (1 if set, 0 if not since the key does not exist)

6. PING/PONG keepalives
	["PING"] -> "PONG"

7. Notifications
	Response only - on a separate TCP connection with no commands
	["pmessage", "*", "__keyevent@0__:set", "csc[63472aad9a791211b792b0a9]wsa.clonbrd.CA:DA:DC:23:8A:61"]

*/

const (
	redisPort = 6379
	bufSize   = 1000000
)

type redisRequest struct {
	reqType     string
	key         string    // key for GET, SET, EXPIRE commands
	requestTime time.Time // when the request was initiated
}

var streamCount int32
var pendingRequests = make(map[string][]redisRequest)
var pendingRequestsLock sync.Mutex
var wg sync.WaitGroup

// redisStreamFactory implements tcpassembly.StreamFactory
type redisStreamFactory struct{}

// redisStream will handle the actual decoding of redis requests.
type redisStream struct {
	net, transport gopacket.Flow
	flowKey        string
	flowLabel      string // what we display in logs
	reader         *tcpreader.ReaderStream
	streamIndex    int32
	clientRequest  bool // true if this is a flow from the client to the server, false otherwise
}

func (*redisStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	dstPortRaw := transport.Dst().Raw()
	dstPort := uint16(dstPortRaw[0])<<8 | uint16(dstPortRaw[1])
	clientRequest := dstPort == redisPort

	var flowKey, flowLabel string
	if clientRequest {
		// dst is the server
		flowKey = fmt.Sprintf("%s:%s->%s:%s", net.Src(), transport.Src(), net.Dst(), transport.Dst())
		flowLabel = flowKey
	} else {
		flowKey = fmt.Sprintf("%s:%s->%s:%s", net.Dst(), transport.Dst(), net.Src(), transport.Src())
		flowLabel = strings.ReplaceAll(flowKey, "->", "<=")
	}

	rstream := &redisStream{
		net:           net,
		transport:     transport,
		flowKey:       flowKey,
		flowLabel:     flowLabel,
		reader:        tcpreader.NewReaderStream(flowLabel),
		streamIndex:   atomic.AddInt32(&streamCount, 1),
		clientRequest: clientRequest,
	}

	// log.Printf("%10d: New flow: req: %s\n", rstream.streamIndex, rstream.flowLabel)

	// Important... we must guarantee that data from the reader stream is read.
	wg.Add(1)
	if rstream.clientRequest {
		go rstream.handleRequests()
	} else {
		go rstream.handleResponses()
	}
	// ReaderStream implements tcpassembly.Stream, so we can return a pointer to it.
	return rstream.reader
}

// read a single simple string "+XXX\n" or a bulk string "$n\nXXXXX\n"
func redisReadString0(line string, timestamp time.Time, tp *tcpreader.ReaderStream) (string, time.Time, error) {
	var err error
	if line[0] == '+' { // beginning of a simple string
		line = line[1:]
	} else if line == "$-1" { // null response (value not found in cache)
		return "not-found", timestamp, nil
	} else if line[0] == '$' { // beginning of a bulk string
		n, _ := strconv.Atoi(line[1:])
		line, timestamp, err = tp.ReadLineN("redisReadString0", n)
		if err == io.EOF {
			return line, timestamp, err
		}
		if len(line) < n {
			log.Fatal(fmt.Sprintf("expected %d characters, got %d characters: %s", n, len(line), line))
		}
	} else if line[0] == ':' {
		line = line[1:] // XXX: we return numbers as strings
	}
	return line, timestamp, nil
}

func redisReadString(tp *tcpreader.ReaderStream) (string, time.Time, error) {
	line, timestamp, err := tp.ReadLine("redisReadString")
	if err == io.EOF {
		return line, timestamp, err
	}
	return redisReadString0(line, timestamp, tp)
}

func redisReadArrayOrString(tp *tcpreader.ReaderStream) ([]string, time.Time, error) {
	line, timestamp, err := tp.ReadLine("redisReadArray")
	if err != nil {
		// We must read until we see an EOF... very important!
		return []string{}, timestamp, err
	}
	// beginning of an array (used for sending commnads or keyevent responses)
	if line[0] == '*' {
		n, _ := strconv.Atoi(line[1:])
		if n < 1 {
			log.Fatalf("redisReadArray: %d elements array: %q", n, line)
		}
		// read n strings
		lines := make([]string, 0, n)
		for i := 0; i < n; i++ {
			line, timestamp, err = redisReadString(tp)
			if err != nil {
				return []string{}, timestamp, err
			}
			lines = append(lines, line)
		}
		return lines, timestamp, nil
	}

	// otherwise it's either a simple string or a bulk string
	line, timestamp, err = redisReadString0(line, timestamp, tp)
	if err != nil {
		log.Fatal("redisReadArray", err)
	}
	return []string{line}, timestamp, err
}

func (s *redisStream) handleRequests() {
	defer wg.Done()
	for {
		lines, timestamp, err := redisReadArrayOrString(s.reader)
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			log.Printf("Req:  %s: received EOF\n", s.flowLabel)
			return
		}
		if err != nil {
			log.Fatalf("Req:   %s: Error reading stream, %v", s.flowLabel, err)
		}
		log.Printf("%s: %s: %v\n", timestamp.Format(time.StampMicro), s.flowLabel, lines)
	}
}

/*
Responses are typically a single value (OK, PONG, get-response) but
may also be arrays if this is a key event
*/
func (s *redisStream) handleResponses() {
	defer wg.Done()
	for {
		lines, timestamp, err := redisReadArrayOrString(s.reader)
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			log.Printf("Resp: %s: received EOF\n", s.flowLabel)
			return
		}
		if err != nil {
			log.Fatalf("Resp:  %s: Error reading stream, %v", s.flowLabel, err)
		}
		log.Printf("%s: %s: %v\n", timestamp.Format(time.StampMicro), s.flowLabel, lines)
	}
}

func main() {
	log.SetFlags(0)

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
	wg.Wait()

	log.Printf("read %d packets, size %d bytes, original size %d bytes\n", count, size, originalSize)
}
