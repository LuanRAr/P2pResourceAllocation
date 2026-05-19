package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

type MessageType string

const MsgAlert MessageType = "ALERT"

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

	//Priority calculado no momento da emissão (1=médio, 2=alto, 3=crítico).
	Priority int `json:"priority"`
}

// -------------geração de IDs únicos de 00 a 100
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
		initIDs() // reinicia quando acabar
	}
	id := availableIDs[0]
	availableIDs = availableIDs[1:]
	return fmt.Sprintf("%02d", id)
}

// -------------cálculo de prioridade
// Converte o valor numérico do sensor em nível discreto de urgência.
func calcPriority(value float64) int {
	switch {
	case value > 95:
		return 3
	case value > 85:
		return 2
	default:
		return 1
	}
}

// -------------main
func main() {
	brokerListRaw := os.Getenv("BROKER_LIST")
	sectorName := os.Getenv("SECTOR_NAME")
	sensorType := os.Getenv("SENSOR_TYPE")

	if brokerListRaw == "" {
		brokerListRaw = "localhost:5000"
	}
	if sectorName == "" {
		sectorName = "Desconhecido"
	}
	if sensorType == "" {
		sensorType = "Generico"
	}

	var brokers []string
	for _, b := range strings.Split(brokerListRaw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}

	sensorID := fmt.Sprintf("SENSOR-%s-%04d", sensorType, rng.Intn(10000))
	fmt.Printf("[%s] iniciado | setor=%s | brokers=%v\n", sensorID, sectorName, brokers)

	for {
		time.Sleep(time.Duration(rng.Intn(10)+5) * time.Second)

		value := rng.Float64() * 100
		if value <= 70 {
			continue // só alerta para valores críticos (> 70)
		}

		alert := AlertPayload{
			AlertID:   newID(),
			SensorID:  sensorID,
			Sector:    sectorName,
			AlertType: sensorType,
			Value:     value,
			Timestamp: time.Now(),
			Priority:  calcPriority(value),
		}

		fmt.Printf("[%s] gerando alerta %s | valor=%.2f | prioridade=%d\n",
			sensorID, alert.AlertID, alert.Value, alert.Priority)

		sendWithFallback(brokers, alert, sensorID)
	}
}

// -------------envio com fallover
// Tenta cada broker da lista em ordem até conseguir enviar.
func sendWithFallback(brokers []string, alert AlertPayload, sensorID string) {
	msg := Message{Type: MsgAlert, Payload: alert}

	for i, addr := range brokers {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			fmt.Printf("[%s] broker %s inacessível (tentativa %d/%d)\n", sensorID, addr, i+1, len(brokers))
			continue
		}

		conn.SetDeadline(time.Now().Add(5 * time.Second))
		encErr := json.NewEncoder(conn).Encode(msg)
		conn.Close()

		if encErr != nil {
			fmt.Printf("[%s] erro ao enviar para %s: %v\n", sensorID, addr, encErr)
			continue
		}

		label := "principal"
		if i > 0 {
			label = fmt.Sprintf("fallback[%d]", i)
		}
		fmt.Printf("[%s] alerta %s → %s (%s) | valor=%.2f prioridade=%d\n",
			sensorID, alert.AlertID, addr, label, alert.Value, alert.Priority)
		return
	}

	fmt.Printf("SENSOR [%s] FALHA: nenhum broker disponível para alerta %s\n", sensorID, alert.AlertID)
}