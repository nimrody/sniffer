package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"github.com/google/gopacket/tcpassembly"
	"github.com/nimrody/my-sinffer/bufio"
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
	key         string // key for GET, SET, EXPIRE commands
	requestTime time.Time
}

var streamCount int32
var pendingRequests = make(map[string][]redisRequest)

// redisStreamFactory implements tcpassembly.StreamFactory
type redisStreamFactory struct{}

// redisStream will handle the actual decoding of redis requests.
type redisStream struct {
	net, transport gopacket.Flow
	flowKey        string
	reader         tcpreader.ReaderStream
	streamIndex    int32
	lastTimestamp  time.Time
	clientRequest  bool // true if this is a flow from the client to the server, false otherwise
}

// Reassembled implements tcpassembly.Stream
func (s *redisStream) Reassembled(segments []tcpassembly.Reassembly) {
	fmt.Printf("%10d: New %d segment: req: %s   %v\n", s.streamIndex, len(segments), s.flowKey, s.clientRequest)
	for i, segment := range segments {
		s.lastTimestamp = segment.Seen
		s.reader.Reassembled(segments[i : i+1])
		fmt.Printf("%10d: processed 1-segment: req: %s   %v\n", s.streamIndex, s.flowKey, s.clientRequest)
	}
}

// ReassemblyComplete implements tcpassembly.Stream
func (s *redisStream) ReassemblyComplete() {
	s.reader.ReassemblyComplete()
}

func (*redisStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	dstPortRaw := transport.Dst().Raw()
	dstPort := uint16(dstPortRaw[0])<<8 | uint16(dstPortRaw[1])
	clientRequest := dstPort == redisPort

	var flowKey string
	if clientRequest {
		// dst is the server
		flowKey = fmt.Sprintf("%s:%s->%s:%s", net.Src(), transport.Src(), net.Dst(), transport.Dst())
	} else {
		flowKey = fmt.Sprintf("%s:%s->%s:%s", net.Dst(), transport.Dst(), net.Src(), transport.Src())
	}

	rstream := &redisStream{
		net:           net,
		transport:     transport,
		flowKey:       flowKey,
		reader:        tcpreader.NewReaderStream(),
		streamIndex:   atomic.AddInt32(&streamCount, 1),
		lastTimestamp: time.Time{},
		clientRequest: clientRequest,
	}

	fmt.Printf("%10d: New flow: req: %s   %v\n", rstream.streamIndex, rstream.flowKey, rstream.clientRequest)

	// Important... we must guarantee that data from the reader stream is read.
	if rstream.clientRequest {
		go rstream.handleRequests()
	} else {
		go rstream.handleResponses()
	}
	// ReaderStream implements tcpassembly.Stream, so we can return a pointer to it.
	return rstream
}

func readLine(tp *bufio.Reader) (string, error) {
	line, err := tp.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	return line, err
}

// read a single simple string "+XXX\n" or a bulk string "$n\nXXXXX\n"
func redisReadString0(line string, tp *bufio.Reader) (string, error) {
	var err error
	if line[0] == '+' { // beginning of a simple string
		line = line[1:]
	} else if line == "$-1" { // null response (value not found in cache)
		return "not-found", nil
	} else if line[0] == '$' { // beginning of a bulk string
		n, _ := strconv.Atoi(line[1:])
		line, err = readLine(tp) // TODO: should read n characters and ignore CRLF
		if err == io.EOF {
			return "", err
		}
		if len(line) < n {
			log.Fatal(fmt.Sprintf("expected %d characters, got %d characters: %s", n, len(line), line))
		}
	} else if line[0] == ':' {
		line = line[1:] // XXX: we return numbers as strings
	}
	return line, nil
}

func redisReadString(tp *bufio.Reader) (string, error) {
	line, err := readLine(tp)
	if err == io.EOF {
		return "", err
	}
	return redisReadString0(line, tp)
}

func redisReadArray(tp *bufio.Reader) ([]string, error) {
	line, err := readLine(tp)
	if err != nil {
		// We must read until we see an EOF... very important!
		return []string{}, err
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
			line, err = redisReadString(tp)
			if err != nil {
				return []string{}, err
			}
			lines = append(lines, line)
		}
		return lines, nil
	}

	// otherwise it's either a simple string or a bulk string
	line, err = redisReadString0(line, tp)
	if err != nil {
		log.Fatal("redisReadArray", err)
	}
	return []string{line}, err
}

func (s *redisStream) handleRequests() {
	tp := bufio.NewReaderSize(&s.reader, bufSize)
	for {
		lines, err := redisReadArray(tp)
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			return
		}
		if err != nil {
			log.Fatal("Error reading stream", err)
		}

		var key string
		command := lines[0]

		if len(lines) > 1 {
			key = lines[1] // key is always the first agument (for GET/SET/EXPIRE)
		}

		req := redisRequest{reqType: command, key: key, requestTime: s.lastTimestamp}

		pendingRequests[s.flowKey] = append(pendingRequests[s.flowKey], req)
		// fmt.Printf("%10d: %s: %s:%s -> %s:%s: %s\n", s.streamIndex, s.lastTimestamp, s.net.Src(), s.transport.Src(), s.net.Dst(), s.transport.Dst(), line)
	}
}

/*
Responses are typically a single value (OK, PONG, get-response) but
may also be arrays if this is a key event
*/
func (s *redisStream) handleResponses() {
	tp := bufio.NewReaderSize(&s.reader, bufSize)
	for {
		lines, err := redisReadArray(tp)
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			return
		}
		if err != nil {
			log.Fatal("Error reading stream", err)
		}

		switch lines[0] {
		case "pmessage":
			// keyevent message - ignore
		default:
			if len(lines) > 1 {
				log.Fatalf("expected 1 value response, got %q", lines)
			}

			found := false
			for i := 0; i < 50; i++ {
				if reqList, ok := pendingRequests[s.flowKey]; ok {
					//fmt.Printf("%10d: %s: response %q\n", s.streamIndex, s.lastTimestamp, lines[0])
					req := reqList[0]
					pendingRequests[s.flowKey] = reqList[1:]
					fmt.Printf("%s: %s %s => %s  latency: %d\n", s.flowKey, req.reqType, req.key, lines[0],
						s.lastTimestamp.UnixMicro()-req.requestTime.UnixMicro())
					found = true
					break
				} else {
					tp.Fill()
					time.Sleep(50 * time.Millisecond)
				}
			}
			if !found {
				log.Printf("map=%v", pendingRequests)
				log.Fatalf("got %s response for flow %s with no matching GET", lines[0], s.flowKey)
			}

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
