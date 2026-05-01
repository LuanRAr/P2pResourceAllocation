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
}

//Drone guarda o próprio estado e a lista de brokers conhecidos
type Drone struct {
	ID      string
	Sector  string
	Brokers []string

	mu        sync.Mutex
	status    string
	missionID string
}

//IDs únicos de 00 a 100
var (
	rng         *rand.Rand
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
		initIDs() //reinicia quando acabar
	}
	id := availableIDs[0]
	availableIDs = availableIDs[1:]
	return fmt.Sprintf("%02d", id)
}

func main() {
	brokerListRaw := os.Getenv("BROKER_LIST") // "IP1:port,IP2:port"
	sectorName    := os.Getenv("SECTOR_NAME")
	dronePrefix   := os.Getenv("DRONE_ID") // prefixo opcional para o ID

	if brokerListRaw == "" { brokerListRaw = "localhost:5000" }
	if sectorName == ""    { sectorName = "Desconhecido" }
	if dronePrefix == ""   { dronePrefix = "DRONE" }

	var brokers []string
	for _, b := range strings.Split(brokerListRaw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}

	d := &Drone{
		ID:      fmt.Sprintf("%s-%s", dronePrefix, newID()),
		Sector:  sectorName,
		Brokers: brokers,
		status:  "AVAILABLE",
	}

	fmt.Printf("[%s] iniciado | setor=%s | brokers=%v\n", d.ID, d.Sector, d.Brokers)

	d.register()

	go d.heartbeatLoop()
	d.missionLoop()
}

//registro
func (d *Drone) register() {
	msg := Message{
		Type: MsgDroneRegister,
		Payload: DronePayload{
			DroneID:   d.ID,
			Sector:    d.Sector,
			Status:    "AVAILABLE",
			Timestamp: time.Now(),
		},
	}
	for {
		if d.send(msg) {
			return
		}
		fmt.Printf("[%s] nenhum broker acessível para registro, aguardando 5s...\n", d.ID)
		time.Sleep(5 * time.Second)
	}
}

//Heartbeat
//envia DRONE_HEARTBEAT a cada 10s
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
				Timestamp: time.Now(),
			},
		}
		if !d.send(msg) {
			fmt.Printf("DRONE [%s] - heartbeat falhou em todos os brokers\n", d.ID)
		}
	}
}

//Loop de missões
func (d *Drone) missionLoop() {
	for {
		time.Sleep(time.Duration(rand.Intn(15)+10) * time.Second)

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

func (d *Drone) reportDone(missionID string) {
	msg := Message{
		Type: MsgDroneDone,
		Payload: DronePayload{
			DroneID:   d.ID,
			Sector:    d.Sector,
			Status:    "AVAILABLE",
			MissionID: missionID,
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

	fmt.Printf("[%s] broker inacessível após missão — buscando broker alternativo\n", d.ID)
	d.register()
}

//Envio com failover
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