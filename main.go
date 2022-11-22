package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/google/gopacket/pcap"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("expected pcap filename argument")
	}

	filename := os.Args[1]

	handleRead, err := pcap.OpenOffline(filename)
	if err != nil {
		log.Fatal("failed to open file:", err)
	}
	defer handleRead.Close()

	var count int
	var size int
	var originalSize int

	for {
		data, ci, err := handleRead.ReadPacketData()
		if err != nil && err != io.EOF {
			log.Fatal("reading packet", err)
		} else if err == io.EOF {
			break
		}
		count++
		size += len(data)
		originalSize += ci.Length
	}
	fmt.Printf("read %d packets, size %d bytes, original size %d bytes\n", count, size, originalSize)
}
