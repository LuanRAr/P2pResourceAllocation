package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type MessageType string

const (
	MsgAlert          MessageType = "ALERT"
	MsgDroneRegister  MessageType = "DRONE_REGISTER"
	MsgDroneHeartbeat MessageType = "DRONE_HEARTBEAT"
	MsgDroneDone      MessageType = "DRONE_DONE"
	MsgPeerPing       MessageType = "PEER_PING"       // broker → broker: sinal de vida
	MsgTakeoverSector MessageType = "TAKEOVER_SECTOR" // broker → broker: assumi setor órfão
)

type Message struct {
	Type    MessageType `json:"type"`
	Payload interface{} `json:"payload"`
}

type AlertPayload struct {
	AlertID   string    `json:"alert_id"`
	SensorID  string    `json:"sensor_id"`
	Sector    string    `json:"sector"`
	AlertType string    `json:"alert_type"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

type DronePayload struct {
	DroneID   string    `json:"drone_id"`
	Sector    string    `json:"sector"`
	Status    string    `json:"status"`
	MissionID string    `json:"mission_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

//Estado dos drones

type DroneState struct {
	DroneID   string
	Status    string    // "AVAILABLE" | "IN_MISSION" | "FAILED"
	MissionID string
	LastSeen  time.Time // atualizado a cada heartbeat
}

//Broker

type Broker struct {
	Name      string
	Port      string
	PeerAddrs []string // IPs:porta dos outros brokers

	dronesMu sync.Mutex
	drones   map[string]*DroneState

	assignedMu sync.Mutex
	assigned   map[string]string 

	peerMu    sync.RWMutex
	peerAlive map[string]bool 
}

func NewBroker(name, port string, peers []string) *Broker {
	b := &Broker{
		Name:      name,
		Port:      port,
		PeerAddrs: peers,
		drones:    make(map[string]*DroneState),
		assigned:  make(map[string]string),
		peerAlive: make(map[string]bool),
	}
	for _, p := range peers {
		b.peerAlive[p] = false
	}
	return b
}

//main

func main() {
	name     := os.Getenv("SECTOR_NAME")
	port     := os.Getenv("PORT")
	peersRaw := os.Getenv("PEER_BROKERS") // "IP1:5000,IP2:5000"	

	if port == "" { port = "5000" }

	var peers []string
	for _, p := range strings.Split(peersRaw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			peers = append(peers, p)
		}
	}

	b := NewBroker(name, port, peers)
	fmt.Printf("=== Broker [%s] na porta %s | peers=%v ===\n", name, port, peers)

	go b.watchDroneHeartbeats() //detecta drones mortos
	go b.watchPeerHeartbeats()  //detecta brokers vizinhos mortos

	//Listener TCP principal
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("[%s] erro ao iniciar: %v\n", name, err)
		os.Exit(1)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go b.handle(conn)
	}
}

//Recepção de mensagens 

func (b *Broker) handle(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Decodifica o envelope genérico sem perder o payload bruto
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		return
	}

	var msgType MessageType
	if err := json.Unmarshal(raw["type"], &msgType); err != nil {
		return
	}

	switch msgType {

	case MsgAlert:
		var p AlertPayload
		if json.Unmarshal(raw["payload"], &p) == nil {
			b.handleAlert(p)
		}

	case MsgDroneRegister:
		var p DronePayload
		if json.Unmarshal(raw["payload"], &p) == nil {
			b.handleDroneRegister(p)
		}

	case MsgDroneHeartbeat:
		var p DronePayload
		if json.Unmarshal(raw["payload"], &p) == nil {
			b.handleDroneHeartbeat(p)
		}

	case MsgDroneDone:
		var p DronePayload
		if json.Unmarshal(raw["payload"], &p) == nil {
			b.handleDroneDone(p)
		}

	case MsgPeerPing:
		// Registra que o broker remetente está vivo
		var p map[string]string
		if json.Unmarshal(raw["payload"], &p) == nil {
			if addr := p["addr"]; addr != "" {
				b.peerMu.Lock()
				b.peerAlive[addr] = true
				b.peerMu.Unlock()
			}
		}

	case MsgTakeoverSector:
		var p map[string]string
		if json.Unmarshal(raw["payload"], &p) == nil {
			fmt.Printf("[%s] broker %s assumiu cobertura do setor %s\n",
				b.Name, p["covered_by"], p["sector"])
		}
	}
}

//uma requisição, um drone

func (b *Broker) handleAlert(p AlertPayload) {
	fmt.Printf("[%s] alerta recebido | id=%s sensor=%s tipo=%s valor=%.2f\n",
		b.Name, p.AlertID, p.SensorID, p.AlertType, p.Value)

	//garante que o mesmo alertID nunca receba dois drones 
	b.assignedMu.Lock()
	if _, already := b.assigned[p.AlertID]; already {
		b.assignedMu.Unlock()
		fmt.Printf("[%s] alerta %s já atendido, ignorando duplicata\n", b.Name, p.AlertID)
		return
	}
	b.assignedMu.Unlock()

	//procura um drone disponível localmente
	b.dronesMu.Lock()
	var chosen *DroneState
	for _, d := range b.drones {
		if d.Status == "AVAILABLE" {
			chosen = d
			break
		}
	}
	if chosen != nil {
		chosen.Status    = "IN_MISSION"
		chosen.MissionID = p.AlertID
	}
	b.dronesMu.Unlock()

	if chosen != nil {
		b.assignedMu.Lock()
		b.assigned[p.AlertID] = chosen.DroneID
		b.assignedMu.Unlock()

		fmt.Printf("[%s] drone %s despachado para alerta %s\n",
			b.Name, chosen.DroneID, p.AlertID)
	} else {
		fmt.Printf("[%s] sem drone disponível para alerta %s\n", b.Name, p.AlertID)
	}
}

//Gestão de drones 

func (b *Broker) handleDroneRegister(p DronePayload) {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()

	if existing, ok := b.drones[p.DroneID]; ok {
		//drone retornou após perder o broker original
		existing.Status   = "AVAILABLE"
		existing.LastSeen = time.Now()
		fmt.Printf("[%s] drone %s re-registrado (voltou de broker alternativo)\n", b.Name, p.DroneID)
	} else {
		b.drones[p.DroneID] = &DroneState{
			DroneID:  p.DroneID,
			Status:   "AVAILABLE",
			LastSeen: time.Now(),
		}
		fmt.Printf("[%s] drone %s registrado | setor=%s\n", b.Name, p.DroneID, p.Sector)
	}
}

func (b *Broker) handleDroneHeartbeat(p DronePayload) {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()

	if d, ok := b.drones[p.DroneID]; ok {
		d.LastSeen  = time.Now()
		d.Status    = p.Status
		d.MissionID = p.MissionID
	} else {
		//drone desconhecido enviou heartbeat
		b.drones[p.DroneID] = &DroneState{
			DroneID:   p.DroneID,
			Status:    p.Status,
			MissionID: p.MissionID,
			LastSeen:  time.Now(),
		}
		fmt.Printf("[%s] drone %s adotado via heartbeat (broker original caiu)\n", b.Name, p.DroneID)
	}
}

func (b *Broker) handleDroneDone(p DronePayload) {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()

	if d, ok := b.drones[p.DroneID]; ok {
		fmt.Printf("[%s] drone %s concluiu missão %s → disponível\n",
			b.Name, p.DroneID, d.MissionID)
		d.Status    = "AVAILABLE"
		d.MissionID = ""
	}
}

//Detecção de drone morto 

//verifica a cada 10 s se algum drone parou de funcionar
func (b *Broker) watchDroneHeartbeats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		b.dronesMu.Lock()
		for _, d := range b.drones {
			if d.Status == "FAILED" {
				continue
			}
			if time.Since(d.LastSeen) > 15*time.Second {
				fmt.Printf("[%s] drone %s presumido MORTO (sem heartbeat há %v) | missão=%s\n",
					b.Name, d.DroneID,
					time.Since(d.LastSeen).Round(time.Second),
					d.MissionID)
				d.Status = "FAILED"

				//Libera a missão para que possa ser atribuída de novo
				if d.MissionID != "" {
					b.assignedMu.Lock()
					delete(b.assigned, d.MissionID)
					b.assignedMu.Unlock()
					d.MissionID = ""
				}
			}
		}
		b.dronesMu.Unlock()
	}
}

//Detecção de broker morto

//faz ping TCP em cada broker vizinho a cada 8 s.
func (b *Broker) watchPeerHeartbeats() {
	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, addr := range b.PeerAddrs {
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)

			b.peerMu.Lock()
			wasAlive := b.peerAlive[addr]

			if err != nil {
				// Peer inacessível
				if wasAlive {
					fmt.Printf("[%s] broker peer %s OFFLINE — assumindo cobertura do setor\n",
						b.Name, addr)
					b.peerAlive[addr] = false
					go b.announceTakeover(addr) // avisa os demais peers
				}
			} else {
				// Peer acessível: envia ping de identificação
				ping := Message{
					Type:    MsgPeerPing,
					Payload: map[string]string{"broker": b.Name, "addr": b.myAddr()},
				}
				conn.SetDeadline(time.Now().Add(4 * time.Second))
				json.NewEncoder(conn).Encode(ping)
				conn.Close()

				if !wasAlive {
					fmt.Printf("[%s] broker peer %s voltou ao ar\n", b.Name, addr)
				}
				b.peerAlive[addr] = true
			}
			b.peerMu.Unlock()
		}
	}
}

// myAddr devolve o endereço público deste broker (usado no ping)
func (b *Broker) myAddr() string {
	myIP := os.Getenv("MY_IP") // definido no docker-compose
	return fmt.Sprintf("%s:%s", myIP, b.Port)
}

// avisa os brokers restantes que este broker cobrindo o setor do peer que caiu
func (b *Broker) announceTakeover(deadAddr string) {
	msg := Message{
		Type: MsgTakeoverSector,
		Payload: map[string]string{
			"sector":     deadAddr,
			"covered_by": b.Name,
		},
	}
	b.peerMu.RLock()
	defer b.peerMu.RUnlock()
	for _, addr := range b.PeerAddrs {
		if addr == deadAddr || !b.peerAlive[addr] {
			continue
		}
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			continue
		}
		conn.SetDeadline(time.Now().Add(4 * time.Second))
		json.NewEncoder(conn).Encode(msg)
		conn.Close()
	}
}