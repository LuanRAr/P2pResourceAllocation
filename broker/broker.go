package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
)

type Message struct {
	SensorID string `json:"sensor_id"`
	Msg      string `json:"msg"`
}

type Broker struct {
	Name  string
	Port  string
	mu    sync.Mutex
	Alive bool
}

func main() {
	name := os.Getenv("SECTOR_NAME")
	port := os.Getenv("PORT")
	if port == "" { port = "5000" }

	fmt.Printf("=== Broker Setor %s Iniciado na porta %s ===\n", name, port)

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("Erro ao iniciar: %v\n", err)
		os.Exit(1)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn, name)
	}
}

func handleConnection(conn net.Conn, sector string) {
	defer conn.Close()
	var msg Message
	err := json.NewDecoder(conn).Decode(&msg)
	if err != nil {
		return
	}
	fmt.Printf("[%s] Recebido de %s: %s\n", sector, msg.SensorID, msg.Msg)
}