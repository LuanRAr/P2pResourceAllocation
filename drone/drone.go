package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type MessageType string

const (
	MsgDroneRegister  MessageType = "DRONE_REGISTER"
	MsgDroneHeartbeat MessageType = "DRONE_HEARTBEAT"
	MsgDroneDone      MessageType = "DRONE_DONE"
	MsgDroneDispatch  MessageType = "DRONE_DISPATCH" // broker → drone: ordem de missão
)

type Message struct {
	Type    MessageType `json:"type"`
	Payload interface{} `json:"payload"`
}

type DronePayload struct {
	DroneID   string    `json:"drone_id"`
	Sector    string    `json:"sector"`
	Status    string    `json:"status"`
	MissionID string    `json:"mission_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	// Addr é enviado no DRONE_REGISTER e no DRONE_HEARTBEAT pra q qualquer broker que receber a mensagem saiba onde alcançar este drone.
	Addr string `json:"addr,omitempty"`
}

//DispatchPayload espelha o struct do broker.
type DispatchPayload struct {
	DroneID   string    `json:"drone_id"`
	MissionID string    `json:"mission_id"`
	Sector    string    `json:"sector"`
	AlertType string    `json:"alert_type"`
	Priority  int       `json:"priority"`
	Timestamp time.Time `json:"timestamp"`
}

// Drone guarda o próprio estado e a lista de brokers conhecidos.
type Drone struct {
	ID      string
	Sector  string
	Addr    string   // IP:porta onde este drone escuta ordens (MY_IP:DRONE_PORT)
	Brokers []string // todos os brokers conhecidos

	mu        sync.Mutex
	status    string
	missionID string
}

// IDs únicos de 00 a 100
var (
	rng          *rand.Rand
	availableIDs []int
)

func init() {
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	initIDs()
}

func initIDs() {
	availableIDs = make([]int, 101)
	for i := 0; i <= 100; i++ {
		availableIDs[i] = i
	}
	rng.Shuffle(len(availableIDs), func(i, j int) {
		availableIDs[i], availableIDs[j] = availableIDs[j], availableIDs[i]
	})
}

func newID() string {
	if len(availableIDs) == 0 {
		initIDs()
	}
	id := availableIDs[0]
	availableIDs = availableIDs[1:]
	return fmt.Sprintf("%02d", id)
}

func main() {
	brokerListRaw := os.Getenv("BROKER_LIST")
	sectorName    := os.Getenv("SECTOR_NAME")
	dronePrefix   := os.Getenv("DRONE_ID")
	myIP          := os.Getenv("MY_IP")
	dronePort     := os.Getenv("DRONE_PORT")

	if brokerListRaw == "" { brokerListRaw = "localhost:5000" }
	if sectorName == ""    { sectorName = "Desconhecido" }
	if dronePrefix == ""   { dronePrefix = "DRONE" }
	if myIP == ""          { myIP = "127.0.0.1" }
	if dronePort == ""     { dronePort = "6000" }

	var brokers []string
	for _, b := range strings.Split(brokerListRaw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}

	d := &Drone{
		ID:      fmt.Sprintf("%s-%s", dronePrefix, newID()),
		Sector:  sectorName,
		Addr:    fmt.Sprintf("%s:%s", myIP, dronePort),
		Brokers: brokers,
		status:  "AVAILABLE",
	}

	fmt.Printf("[%s] iniciado | setor=%s | addr=%s | brokers=%v\n",
		d.ID, d.Sector, d.Addr, d.Brokers)

	//Inicia o servidor TCP antes do registro para garantir que o broker já consiga alcançar o drone quando receber o DRONE_REGISTER.
	go d.serveDispatches(dronePort)

	// Registra em TODOS os brokers da lista para que qualquer um que receba um alerta já saiba o endereço deste drone
	d.registerAll()

	go d.heartbeatLoop()
	d.missionLoop()
}

//-------------registro em todos os brokers
//envia o DRONE_REGISTER para cada broker da lista independentemente
func (d *Drone) registerAll() {
	payload := DronePayload{
		DroneID:   d.ID,
		Sector:    d.Sector,
		Status:    "AVAILABLE",
		Addr:      d.Addr,
		Timestamp: time.Now(),
	}
	msg := Message{Type: MsgDroneRegister, Payload: payload}

	atLeastOne := false
	for _, addr := range d.Brokers {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			fmt.Printf("[%s] registro: broker %s inacessível\n", d.ID, addr)
			continue
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			fmt.Printf("[%s] registro: erro ao enviar para %s: %v\n", d.ID, addr, err)
			conn.Close()
			continue
		}
		conn.Close()
		fmt.Printf("[%s] registrado em %s\n", d.ID, addr)
		atLeastOne = true
	}

	if !atLeastOne {
		fmt.Printf("[%s] nenhum broker acessível, tentando novamente em 5s...\n", d.ID)
		time.Sleep(5 * time.Second)
		d.registerAll()
	}
}

//-------------servidor TCP: recebe ordens do broker
// O drone escuta na DRONE_PORT, esperando DRONE_DISPATCHs do broker
func (d *Drone) serveDispatches(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("[%s] erro ao abrir porta de despacho %s: %v\n", d.ID, port, err)
		return
	}
	fmt.Printf("[%s] aguardando ordens na porta %s\n", d.ID, port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go d.handleDispatch(conn)
	}
}

func (d *Drone) handleDispatch(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		return
	}

	var msgType MessageType
	if err := json.Unmarshal(raw["type"], &msgType); err != nil {
		return
	}
	if msgType != MsgDroneDispatch {
		return
	}

	var p DispatchPayload
	if err := json.Unmarshal(raw["payload"], &p); err != nil {
		return
	}

	if p.DroneID != d.ID {
		fmt.Printf("[%s] ordem recebida para drone errado (%s) — ignorando\n", d.ID, p.DroneID)
		return
	}

	d.mu.Lock()
	if d.status == "IN_MISSION" {
		fmt.Printf("[%s] já em missão %s — ordem para %s descartada\n",
			d.ID, d.missionID, p.MissionID)
		d.mu.Unlock()
		return
	}
	d.status    = "IN_MISSION"
	d.missionID = p.MissionID
	d.mu.Unlock()

	fmt.Printf("[%s] MISSÃO RECEBIDA | id=%s | setor=%s | tipo=%s | prioridade=%d\n",
		d.ID, p.MissionID, p.Sector, p.AlertType, p.Priority)
}

//-------------heartbeat: envia para TODOS os brokers
// heartbeat é enviado para todos os brokers vivos. Assim cada broker mantém o de qual deles detém o token no momento.
func (d *Drone) heartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		d.mu.Lock()
		status    := d.status
		missionID := d.missionID
		d.mu.Unlock()

		msg := Message{
			Type: MsgDroneHeartbeat,
			Payload: DronePayload{
				DroneID:   d.ID,
				Sector:    d.Sector,
				Status:    status,
				MissionID: missionID,
				Addr:      d.Addr,
				Timestamp: time.Now(),
			},
		}

		sent := 0
		for _, addr := range d.Brokers {
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				continue
			}
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			if json.NewEncoder(conn).Encode(msg) == nil {
				sent++
			}
			conn.Close()
		}
		if sent == 0 {
			fmt.Printf("[%s] heartbeat falhou em todos os brokers\n", d.ID)
		}
	}
}

//-------------loop de missões
func (d *Drone) missionLoop() {
	for {
		time.Sleep(time.Duration(rand.Intn(7)+10) * time.Second)

		d.mu.Lock()
		status    := d.status
		missionID := d.missionID
		d.mu.Unlock()

		if status != "IN_MISSION" || missionID == "" {
			continue
		}

		dur := time.Duration(rand.Intn(10)+10) * time.Second
		fmt.Printf("[%s] executando missão %s por %v\n", d.ID, missionID, dur)
		time.Sleep(dur)

		d.reportDone(missionID)
	}
}

//-------------conclusão de missão
func (d *Drone) reportDone(missionID string) {
	msg := Message{
		Type: MsgDroneDone,
		Payload: DronePayload{
			DroneID:   d.ID,
			Sector:    d.Sector,
			Status:    "AVAILABLE",
			MissionID: missionID,
			Addr:      d.Addr,
			Timestamp: time.Now(),
		},
	}

	if d.send(msg) {
		fmt.Printf("[%s] missão %s concluída e reportada\n", d.ID, missionID)
		d.mu.Lock()
		d.status    = "AVAILABLE"
		d.missionID = ""
		d.mu.Unlock()
		return
	}

	fmt.Printf("[%s] broker inacessível após missão — re-registrando em todos\n", d.ID)
	d.mu.Lock()
	d.status    = "AVAILABLE"
	d.missionID = ""
	d.mu.Unlock()
	d.registerAll()
}

//-------------envio com failover (para ao primeiro sucesso)
//mensagens p2p como DRONE_DONE onde qualquer broker que receba é suficiente
func (d *Drone) send(msg Message) bool {
	for i, addr := range d.Brokers {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			if i == 0 {
				fmt.Printf("[%s] broker principal %s inacessível, tentando fallback\n", d.ID, addr)
			}
			continue
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		encErr := json.NewEncoder(conn).Encode(msg)
		conn.Close()
		if encErr == nil {
			return true
		}
	}
	return false
}