package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type Sensor struct {
	ID          string
	BrokerList  []string
	CurrentIdx  int
	Conn        net.Conn
}

func main() {
	// Exemplo: BROKER_LIST=172.20.0.2:5000,172.20.0.3:5000
	list := os.Getenv("BROKER_LIST")
	s := &Sensor{
		ID:         "SENSOR-01",
		BrokerList: strings.Split(list, ","),
		CurrentIdx: 0,
	}

	for {
		err := s.ensureConnection()
		if err != nil {
			fmt.Printf("[!] Falha crítica: Nenhum broker disponível. Tentando novamente em 5s...\n")
			time.Sleep(5 * time.Second)
			continue
		}

		// Simula envio de dados
		alert := map[string]string{"sensor_id": s.ID, "msg": "status_ok"}
		err = json.NewEncoder(s.Conn).Encode(alert)
		
		if err != nil {
			fmt.Println("[X] Conexão perdida com o Broker. Iniciando failover...")
			s.Conn.Close()
			s.Conn = nil
			s.rotateBroker() // Muda para o próximo IP da lista
		}

		time.Sleep(2 * time.Second)
	}
}

func (s *Sensor) ensureConnection() error {
	if s.Conn != nil {
		return nil
	}

	for i := 0; i < len(s.BrokerList); i++ {
		addr := s.BrokerList[s.CurrentIdx]
		fmt.Printf("[...] Tentando conectar ao Broker: %s\n", addr)
		
		conn, err := néet.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			fmt.Printf("[V] Conectado ao Broker: %s\n", addr)
			s.Conn = conn
			return nil
		}

		fmt.Printf("[!] Broker %s indisponível.\n", addr)
		s.rotateBroker()
	}
	return fmt.Errorf("all brokers down")
}

func (s *Sensor) rotateBroker() {
	s.CurrentIdx = (s.CurrentIdx + 1) % len(s.BrokerList)
}