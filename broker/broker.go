package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// -------------tipos de mensagem
type MessageType string

const (
	MsgAlert          MessageType = "ALERT"
	MsgDroneRegister  MessageType = "DRONE_REGISTER"
	MsgDroneHeartbeat MessageType = "DRONE_HEARTBEAT"
	MsgDroneDone      MessageType = "DRONE_DONE"

	MsgPeerPing        MessageType = "PEER_PING"        //broker → broker: sinal de vida
	MsgCoberturaSector MessageType = "Cobertura_SECTOR" //broker → broker: aviso de cobertura

	MsgToken         MessageType = "TOKEN"          //passagem do token
	MsgStateUpdate   MessageType = "STATE_UPDATE"   //broker → peers: réplica do estado global
	MsgDroneDispatch MessageType = "DRONE_DISPATCH" //broker → drone: ordem de missão
)

// -------------estruturas de mensagem
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
	Priority  int       `json:"priority"`
}

// -------------ordenação da fila de alertas por prioridade + timestamp
type AlertQueue []AlertPayload

func (q AlertQueue) Len() int {
	return len(q)
}

func (q AlertQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q AlertQueue) Less(i, j int) bool {
	if q[i].Priority != q[j].Priority {
		return q[i].Priority > q[j].Priority //maior prioridade primeiro
	}
	return q[i].Timestamp.Before(q[j].Timestamp) //mais antigo primeiro
}

type DronePayload struct {
	DroneID   string    `json:"drone_id"`
	Sector    string    `json:"sector"`
	Status    string    `json:"status"`
	MissionID string    `json:"mission_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	//Addr é preenchido no DRONE_REGISTER para que o broker saiba onde abrir conexão TCP de volta ao drone (IP:DRONE_PORT).
	Addr string `json:"addr,omitempty"`
}

// DispatchPayload é a mensagem enviada diretamente do broker ao drone para iniciá-lo em uma missão específica
type DispatchPayload struct {
	DroneID   string    `json:"drone_id"`
	MissionID string    `json:"mission_id"`
	Sector    string    `json:"sector"` // setor da ocorrência
	AlertType string    `json:"alert_type"`
	Priority  int       `json:"priority"`
	Timestamp time.Time `json:"timestamp"`
}

// MissionStatus representa o ciclo de vida de uma missão no log histórico.
type MissionStatus string

const (
	MissionPending    MissionStatus = "PENDENTE"
	MissionDispatched MissionStatus = "DESPACHADA"
	MissionDone       MissionStatus = "CONCLUÍDA"
	MissionFailed     MissionStatus = "FALHA"
	MissionOrphan     MissionStatus = "ÓRFÃ"
)

// MissionRecord é uma entrada imutável no log histórico de missões.
// Só Status, DroneID e FinishedAt são atualizados após a criação.
type MissionRecord struct {
	AlertID      string        `json:"alert_id"`
	Sector       string        `json:"sector"`
	AlertType    string        `json:"alert_type"`
	Priority     int           `json:"priority"`
	DroneID      string        `json:"drone_id,omitempty"`
	Status       MissionStatus `json:"status"`
	CreatedAt    time.Time     `json:"created_at"`
	DispatchedAt *time.Time    `json:"dispatched_at,omitempty"`
	FinishedAt   *time.Time    `json:"finished_at,omitempty"`
}

// carrega o estado global compartilhado pelo anel
type TokenPayload struct {
	PendingAlerts []AlertPayload `json:"pending_alerts"` //fila global de alertas ainda sem drone atribuído

	Assigned map[string]string `json:"assigned"` //mapa de alert_id → drone_id para alertas já atribuídos

	//mapa de drone_id → status global (AVAILABLE IN_MISSION FAILED)
	DroneStatus map[string]string `json:"drone_status"`

	//número de voltas completas (debug / detecção de token perdido)
	Round uint64 `json:"round"`

	// Log histórico completo de todas as missões
	MissionLog map[string]*MissionRecord `json:"mission_log"`
}

// -------------estado do drone
type DroneState struct {
	DroneID   string
	Sector    string //setor base do drone (para preferência geográfica)
	Addr      string //IP:porta onde o drone escuta DRONE_DISPATCH
	Status    string //AVAILABLE, IN_MISSION, FAILED
	MissionID string
	LastSeen  time.Time
}

// -------------broker
type Broker struct {
	Name      string
	Port      string
	MyAddr    string   //IP:porta deste broker (para o ping)
	RingAddrs []string //todos os endereços do anel, em ordem

	//estado local dos drones registrados neste broker
	dronesMu sync.Mutex
	drones   map[string]*DroneState

	//alertas recebidos localmente enquanto não temos o token
	localAlertsMu sync.Mutex
	localAlerts   []AlertPayload

	//alertas órfãos: missões q o drone foi marcado como FAILED são reinseridos na fila global com prioridade máxima no próximo round
	orphanMu     sync.Mutex
	orphanAlerts []AlertPayload

	//token ring
	tokenMu         sync.Mutex
	hasToken        bool
	lastTokenSeen   time.Time
	tokenData       TokenPayload //última versão conhecida do token
	backupTokenData TokenPayload //backup replicado via broadcast dos peers

	//guarda o estado dos outros brokers do sistema
	peerMu    sync.RWMutex
	peerAlive map[string]bool

	//Canal para acordar o loop do token quando um alerta chega
	alertReady chan struct{}

	// Guard contra múltiplas instâncias simultâneas de tokenLoop.
	tokenLoopRunning bool
}

func NewBroker(name, port, myAddr string, ring []string) *Broker {
	b := &Broker{
		Name:          name,
		Port:          port,
		MyAddr:        myAddr,
		RingAddrs:     ring,
		drones:        make(map[string]*DroneState),
		peerAlive:     make(map[string]bool),
		alertReady:    make(chan struct{}, 64),
		lastTokenSeen: time.Now(),
		tokenData: TokenPayload{
			PendingAlerts: []AlertPayload{},
			Assigned:      make(map[string]string),
			DroneStatus:   make(map[string]string),
			MissionLog:    make(map[string]*MissionRecord),
		},
	}
	//inicialmente considera todos os peers offline até o ping confirmar
	for _, addr := range ring {
		if addr != myAddr {
			b.peerAlive[addr] = false
		}
	}
	return b
}

// -------------main
func main() {
	name := os.Getenv("SECTOR_NAME")
	port := os.Getenv("PORT")
	myIP := os.Getenv("MY_IP")
	ringRaw := os.Getenv("RING_ADDRS") //anel completo em ordem: "IP1:5000,IP2:5000,IP3:5000"

	if port == "" {
		port = "5000"
	}
	myAddr := fmt.Sprintf("%s:%s", myIP, port)

	var ring []string
	for _, addr := range strings.Split(ringRaw, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			ring = append(ring, addr)
		}
	}

	b := NewBroker(name, port, myAddr, ring)
	fmt.Printf("=== Broker [%s] | addr=%s | anel=%v ===\n", name, myAddr, ring)

	go b.watchDroneHeartbeats() //detecta drones mortos
	go b.peerHeartbeats()       //detecta brokers vizinhos mortos + envia ping
	go b.tokenTimeoutGuard()    //regenera token se ficar perdido

	//Apenas o primeiro nó do anel inicia com o token
	if len(ring) > 0 && ring[0] == myAddr {
		fmt.Printf("[%s] sou o nó inicial — gerando token\n", name)
		b.tokenMu.Lock()
		b.hasToken = true
		b.lastTokenSeen = time.Now()
		b.tokenMu.Unlock()
		go b.tokenLoop()
	}

	//listener TCP principal
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

// -------------recepção de mensagens
func (b *Broker) handle(conn net.Conn) {
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
		var p map[string]string
		if json.Unmarshal(raw["payload"], &p) == nil {
			if addr := p["addr"]; addr != "" {
				b.peerMu.Lock()
				b.peerAlive[addr] = true
				b.peerMu.Unlock()
			}
		}

	case MsgCoberturaSector:
		var p map[string]string
		if json.Unmarshal(raw["payload"], &p) == nil {
			fmt.Printf("[%s] broker %s assumiu cobertura do setor %s\n",
				b.Name, p["covered_by"], p["sector"])
		}

	//-------------token ring: recebimento do token
	case MsgToken:
		var p TokenPayload
		if json.Unmarshal(raw["payload"], &p) == nil {
			b.receberToken(p)
		}

	//-------------replicação de estado: atualiza backup local
	case MsgStateUpdate:
		var p TokenPayload
		if json.Unmarshal(raw["payload"], &p) == nil {
			b.tokenMu.Lock()
			// Só substitui se a versão recebida for mais recente (Round maior),
			if p.Round > b.backupTokenData.Round {
				b.backupTokenData = p
				fmt.Printf("[%s] backup de estado atualizado (round=%d, pendentes=%d)\n",
					b.Name, p.Round, len(p.PendingAlerts))
			}
			b.tokenMu.Unlock()
		}	
	}
}

// -------------alerta: enfileira localmente e acorda o loop do token
func (b *Broker) handleAlert(p AlertPayload) {
	fmt.Printf("[%s] alerta recebido | id=%s sensor=%s tipo=%s valor=%.2f\n",
		b.Name, p.AlertID, p.SensorID, p.AlertType, p.Value)

	b.localAlertsMu.Lock()
	b.localAlerts = append(b.localAlerts, p)
	b.localAlertsMu.Unlock()

	// Notifica o loop do token (se este broker o detiver agora)
	select {
	case b.alertReady <- struct{}{}:
	default:
	}
}

// -------------gestão de drones
func (b *Broker) handleDroneRegister(p DronePayload) {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()

	if existing, ok := b.drones[p.DroneID]; ok {
		existing.Status = "AVAILABLE"
		existing.LastSeen = time.Now()
		//Atualiza endereço caso o drone tenha reiniciado em IP diferente
		if p.Addr != "" {
			existing.Addr = p.Addr
		}
		if p.Sector != "" {
			existing.Sector = p.Sector
		}
		fmt.Printf("[%s] drone %s re-registrado | addr=%s\n", b.Name, p.DroneID, existing.Addr)
	} else {
		b.drones[p.DroneID] = &DroneState{
			DroneID:  p.DroneID,
			Sector:   p.Sector,
			Addr:     p.Addr,
			Status:   "AVAILABLE",
			LastSeen: time.Now(),
		}
		fmt.Printf("[%s] drone %s registrado | setor=%s | addr=%s\n", b.Name, p.DroneID, p.Sector, p.Addr)
	}
}

func (b *Broker) handleDroneHeartbeat(p DronePayload) {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()

	if d, ok := b.drones[p.DroneID]; ok {
		d.LastSeen = time.Now()
		d.Status = p.Status
		d.MissionID = p.MissionID
		if p.Addr != "" {
			d.Addr = p.Addr
		}
	} else {
		// Drone adotado (broker original caiu)
		b.drones[p.DroneID] = &DroneState{
			DroneID:   p.DroneID,
			Sector:    p.Sector,
			Addr:      p.Addr,
			Status:    p.Status,
			MissionID: p.MissionID,
			LastSeen:  time.Now(),
		}
		fmt.Printf("[%s] drone %s adotado via heartbeat | addr=%s\n", b.Name, p.DroneID, p.Addr)
	}
}

func (b *Broker) handleDroneDone(p DronePayload) {
	b.dronesMu.Lock()
	if d, ok := b.drones[p.DroneID]; ok {
		d.Status = "AVAILABLE"
		d.MissionID = ""
	}
	b.dronesMu.Unlock()

	// Atualiza o estado global imediatamente para que cleanAssigned funcione corretamente no próximo round.
	b.tokenMu.Lock()
	if b.tokenData.DroneStatus == nil {
		b.tokenData.DroneStatus = make(map[string]string)
	}
	b.tokenData.DroneStatus[p.DroneID] = "AVAILABLE"

	// Marca a missão como CONCLUÍDA no log histórico
	nowDone := time.Now()
	if p.MissionID != "" {
		if b.tokenData.MissionLog == nil {
			b.tokenData.MissionLog = make(map[string]*MissionRecord)
		}
		b.logMission(p.MissionID, func(r *MissionRecord) {
			r.Status = MissionDone
			r.FinishedAt = &nowDone
		})
	}
	b.tokenMu.Unlock()
}

// mergeMissionLogs combina dois MissionLogs preservando, para cada alert_id, a entrada com o status mais avançado no ciclo de vida.
func mergeMissionLogs(local, incoming map[string]*MissionRecord) map[string]*MissionRecord {
	statusRank := map[MissionStatus]int{
		MissionPending:    0,
		MissionDispatched: 1,
		MissionOrphan:     2,
		MissionFailed:     3,
		MissionDone:       4,
	}
	if local == nil {
		local = make(map[string]*MissionRecord)
	}
	result := make(map[string]*MissionRecord, len(local))
	for id, r := range local {
		copy := *r
		result[id] = &copy
	}
	for id, inc := range incoming {
		existing, ok := result[id]
		if !ok {
			copy := *inc
			result[id] = &copy
			continue
		}
		// Mantém o status mais avançado no ciclo de vida
		if statusRank[inc.Status] > statusRank[existing.Status] {
			*existing = *inc
		}
	}
	return result
}

// -------------token ring: recebimento
//broker atualiza seu estado local com as informações do token, absorve os alertas locais pendentes na fila global e inicia envio de drones
func (b *Broker) receberToken(p TokenPayload) {
	b.tokenMu.Lock()
	defer b.tokenMu.Unlock()

	if b.hasToken {
		// Duplicata (pode acontecer durante regeneração): descarta
		fmt.Printf("[%s] token duplicado recebido — descartando\n", b.Name)
		return
	}

	fmt.Printf("[%s] token recebido | round=%d | pendentes=%d\n",
		b.Name, p.Round, len(p.PendingAlerts))

	b.hasToken = true
	b.lastTokenSeen = time.Now()

	// Mescla o DroneStatus recebido com o estado local
	incomingStatus := p.DroneStatus
	p.DroneStatus = make(map[string]string, len(incomingStatus))
	for droneID, incomingS := range incomingStatus {
		localS, hasLocal := b.tokenData.DroneStatus[droneID]
		if hasLocal && localS == "AVAILABLE" && incomingS != "AVAILABLE" {
			p.DroneStatus[droneID] = "AVAILABLE"
		} else {
			p.DroneStatus[droneID] = incomingS
		}
	}
	// Garante que drones só conhecidos localmente também entrem no token
	for droneID, localS := range b.tokenData.DroneStatus {
		if _, exists := p.DroneStatus[droneID]; !exists {
			p.DroneStatus[droneID] = localS
		}
	}

	//Merge do MissionLog: preserva a entrada mais recente entre o log local e o que chegou no token, para nunca perder histórico quando um broker volta ao ar com um log desatualizado.
	p.MissionLog = mergeMissionLogs(b.tokenData.MissionLog, p.MissionLog)
	b.tokenData = p

	go b.tokenLoop()
}

// -------------impressão da fila de alertas
// Exibe o estado completo da fila no momento em que o broker detém o token.
func (b *Broker) printQueue() {
	pending := b.tokenData.PendingAlerts
	assigned := b.tokenData.Assigned
	droneStatus := b.tokenData.DroneStatus

	fmt.Printf("\n[%s] ╔══════════════════ FILA DE ALERTAS (round=%d) ══════════════════╗\n",
		b.Name, b.tokenData.Round)

	if len(pending) == 0 && len(assigned) == 0 {
		fmt.Printf("[%s] ║  (fila vazia)                                                    ║\n", b.Name)
	}

	//Pendentes (ainda sem drone atribuído)
	if len(pending) > 0 {
		fmt.Printf("[%s] ║  PENDENTES (%d):\n", b.Name, len(pending))
		for i, a := range pending {
			age := time.Since(a.Timestamp).Round(time.Second)
			fmt.Printf("[%s] ║    [%d] id=%-4s setor=%-8s tipo=%-14s pri=%d idade=%v\n",
				b.Name, i+1, a.AlertID, a.Sector, a.AlertType, a.Priority, age)
		}
	}

	//Atribuídos (drone despachado, missão em andamento)
	if len(assigned) > 0 {
		fmt.Printf("[%s] ║  EM MISSÃO (%d):\n", b.Name, len(assigned))
		for alertID, droneID := range assigned {
			status := droneStatus[droneID]
			fmt.Printf("[%s] ║    id=%-4s → drone=%-20s status=%s\n",
				b.Name, alertID, droneID, status)
		}
	}

	fmt.Printf("[%s] ╚════════════════════════════════════════════════════════════════╝\n\n", b.Name)
}

// -------------token ring: loop de processamento
// Enquanto o broker tiver o token.
func (b *Broker) tokenLoop() {
	//Guard garante que apenas uma instância de tokenLoop roda por vez.
	b.tokenMu.Lock()
	if b.tokenLoopRunning {
		b.tokenMu.Unlock()
		fmt.Printf("[%s] tokenLoop já em execução — descartando instância duplicada\n", b.Name)
		return
	}
	b.tokenLoopRunning = true
	b.tokenMu.Unlock()

	defer func() {
		b.tokenMu.Lock()
		b.tokenLoopRunning = false
		b.tokenMu.Unlock()
	}()

	//Pequena espera inicial para absorver alertas que chegam em rajada
	time.Sleep(200 * time.Millisecond)

	//Drena o canal alertReady para descartar notificações acumuladas de rodadas anteriores
	for {
		select {
		case <-b.alertReady:
		default:
			goto drained
		}
	}
drained:

	// Loop de monitorização ativa sem reter locks desnecessários (Prevenção de Deadlocks)
	for {
		b.localAlertsMu.Lock()
		temAlertas := len(b.localAlerts) > 0
		b.localAlertsMu.Unlock()

		b.orphanMu.Lock()
		temOrfaos := len(b.orphanAlerts) > 0
		b.orphanMu.Unlock()

		b.tokenMu.Lock()
		temPendentes := len(b.tokenData.PendingAlerts) > 0
		b.tokenMu.Unlock()

		if temAlertas || temOrfaos || temPendentes {
			break // Sai do loop para processar o trabalho existente
		}

		// Aguarda novos dados ou avança por timeout para manter a circulação de estado
		select {
		case <-b.alertReady:
			// Acordado por novo alerta local
		case <-time.After(2 * time.Second):
			goto process
		}
	}

process:
	// Bloqueio atómico unificado para todo o ciclo de processamento do Token
	b.tokenMu.Lock()

	// 1. Absorve alertas órfãos com prioridade máxima
	b.orphanMu.Lock()
	for _, orphan := range b.orphanAlerts {
		if _, already := b.tokenData.Assigned[orphan.AlertID]; !already {
			delete(b.tokenData.Assigned, orphan.AlertID)
			b.tokenData.PendingAlerts = append(b.tokenData.PendingAlerts, orphan)
			
			orphanSnap := orphan
			b.logMission(orphan.AlertID, func(r *MissionRecord) {
				r.Status = MissionOrphan
				if r.Sector == "" { r.Sector = orphanSnap.Sector }
				if r.AlertType == "" { r.AlertType = orphanSnap.AlertType }
				r.Priority = orphanSnap.Priority
			})
			fmt.Printf("[%s] alerta órfão %s reinserido na fila (prioridade=%d)\n",
				b.Name, orphan.AlertID, orphan.Priority)
		}
	}
	b.orphanAlerts = nil
	b.orphanMu.Unlock()

	// 2. Absorve alertas locais novos na fila global
	b.localAlertsMu.Lock()
	for _, a := range b.localAlerts {
		if _, already := b.tokenData.Assigned[a.AlertID]; !already {
			b.tokenData.PendingAlerts = append(b.tokenData.PendingAlerts, a)
			alertSnap := a
			b.logMission(a.AlertID, func(r *MissionRecord) {
				if r.Status == "" {
					r.Sector = alertSnap.Sector
					r.AlertType = alertSnap.AlertType
					r.Priority = alertSnap.Priority
					r.Status = MissionPending
				}
			})
		}
	}
	b.localAlerts = nil
	b.localAlertsMu.Unlock()

	// 3. Sincroniza estado global dos drones
	b.dronesMu.Lock()
	for droneID, d := range b.drones {
		globalStatus, hasGlobal := b.tokenData.DroneStatus[droneID]
		if !hasGlobal || globalStatus != "AVAILABLE" {
			b.tokenData.DroneStatus[droneID] = d.Status
		}
	}
	for droneID, globalStatus := range b.tokenData.DroneStatus {
		if local, ok := b.drones[droneID]; ok {
			if local.Status != "FAILED" {
				local.Status = globalStatus
			}
		}
	}
	b.dronesMu.Unlock()

	// 4. Recupera missões de drones que falharam mantendo o tipo original do alerta
	for alertID, droneID := range b.tokenData.Assigned {
		if status, ok := b.tokenData.DroneStatus[droneID]; ok && status == "FAILED" {
			alreadyPending := false
			for _, p := range b.tokenData.PendingAlerts {
				if p.AlertID == alertID {
					alreadyPending = true
					break
				}
			}
			if !alreadyPending {
				// CORREÇÃO: Recupera o tipo real do log histórico para não estragar a telemetria
				originalType := "Recuperado"
				if rec, exists := b.tokenData.MissionLog[alertID]; exists {
					originalType = rec.AlertType
				}

				recovered := AlertPayload{
					AlertID:   alertID,
					SensorID:  droneID,
					AlertType: originalType,
					Timestamp: time.Now(),
					Priority:  3, // Mantém prioridade máxima para reatendimento emergencial
				}
				b.tokenData.PendingAlerts = append(b.tokenData.PendingAlerts, recovered)
				delete(b.tokenData.Assigned, alertID)
				fmt.Printf("[%s] TOKEN → alerta %s recuperado devido a falha do drone %s — reinserido\n",
					b.Name, alertID, droneID)
			}
		}
	}

	// 5. Ordena a fila global por Prioridade (DESC) e Timestamp (ASC)
	sort.Sort(AlertQueue(b.tokenData.PendingAlerts))

	b.printQueue()
	b.printMissionLog()

	// 6. Alocação e Despacho SÍNCRONO de missões aos Drones
	var stillPending []AlertPayload
	for _, alert := range b.tokenData.PendingAlerts {
		if _, already := b.tokenData.Assigned[alert.AlertID]; already {
			continue
		}

		droneID := b.droneDisp(alert.Sector)
		if droneID == "" {
			stillPending = append(stillPending, alert)
			fmt.Printf("[%s] sem drone disponível para alerta %s — mantendo na fila\n", b.Name, alert.AlertID)
			continue
		}

		// Pré-reserva atómica no Token
		b.tokenData.Assigned[alert.AlertID] = droneID
		b.tokenData.DroneStatus[droneID] = "IN_MISSION"

		now := time.Now()
		droneSnap := droneID
		alertSnap2 := alert
		b.logMission(alert.AlertID, func(r *MissionRecord) {
			r.DroneID = droneSnap
			r.Sector = alertSnap2.Sector
			r.AlertType = alertSnap2.AlertType
			r.Priority = alertSnap2.Priority
			r.Status = MissionDispatched
			r.DispatchedAt = &now
		})

		// Se o drone falhar na rede, a transação é revertida localmente de imediato.
		if b.enviaDrone(droneID, alert) {
			b.dronesMu.Lock()
			if d, ok := b.drones[droneID]; ok {
				d.Status = "IN_MISSION"
				d.MissionID = alert.AlertID
			}
			b.dronesMu.Unlock()
			fmt.Printf("[%s] TOKEN → Despacho efetuado com sucesso para o drone %s\n", b.Name, droneID)
		} else {
			// Reversão imediata em caso de falha de rede física
			delete(b.tokenData.Assigned, alert.AlertID)
			b.tokenData.DroneStatus[droneID] = "FAILED"

			b.dronesMu.Lock()
			if d, ok := b.drones[droneID]; ok {
				d.Status = "FAILED"
				d.LastSeen = time.Now().Add(-20 * time.Second) // Força timeout local
			}
			b.dronesMu.Unlock()

			nowFail := time.Now()
			b.logMission(alert.AlertID, func(r *MissionRecord) {
				r.Status = MissionFailed
				r.FinishedAt = &nowFail
			})

			stillPending = append(stillPending, alert)
			fmt.Printf("[%s] falha crítica ao ligar ao drone %s — transação abortada e alerta reinserido\n", b.Name, droneID)
		}
	}
	b.tokenData.PendingAlerts = stillPending
	b.tokenData.Round++

	b.cleanAssigned()

	// Libera a posse do token local e prepara envio para a rede
	b.hasToken = false
	tokenSnapshot := b.tokenData
	b.tokenMu.Unlock()

	// 7. Replica o estado atualizado em broadcast para redundância em caso de quedas
	b.broadcastState(tokenSnapshot)

	time.Sleep(300 * time.Millisecond)

	// 8. Passa o token para o próximo nó ativo do anel
	b.passToken(tokenSnapshot)
}

// -------------log histórico de missões
//cria ou atualiza a entrada de uma missão no MissionLog do token.
func (b *Broker) logMission(alertID string, update func(r *MissionRecord)) {
	if b.tokenData.MissionLog == nil {
		b.tokenData.MissionLog = make(map[string]*MissionRecord)
	}
	r, ok := b.tokenData.MissionLog[alertID]
	if !ok {
		r = &MissionRecord{AlertID: alertID, CreatedAt: time.Now()}
		b.tokenData.MissionLog[alertID] = r
	}
	update(r)
}

//printMissionLog exibe o log histórico completo de missões.
func (b *Broker) printMissionLog() {
	log := b.tokenData.MissionLog
	if len(log) == 0 {
		return
	}
	fmt.Printf("\n[%s] ╔══════════════════ HISTÓRICO DE MISSÕES (round=%d) ══════════════════╗\n",
		b.Name, b.tokenData.Round)
	// Agrupa por status para facilitar leitura
	groups := map[MissionStatus][]*MissionRecord{}
	for _, r := range log {
		groups[r.Status] = append(groups[r.Status], r)
	}
	for _, status := range []MissionStatus{MissionDispatched, MissionPending, MissionDone, MissionFailed, MissionOrphan} {
		records := groups[status]
		if len(records) == 0 {
			continue
		}
		fmt.Printf("[%s] ║  %s (%d):\n", b.Name, status, len(records))
		for _, r := range records {
			duration := ""
			if r.FinishedAt != nil && r.DispatchedAt != nil {
				duration = fmt.Sprintf(" duração=%v", r.FinishedAt.Sub(*r.DispatchedAt).Round(time.Second))
			}
			fmt.Printf("[%s] ║    id=%-4s setor=%-8s tipo=%-14s pri=%d drone=%-20s%s\n",
				b.Name, r.AlertID, r.Sector, r.AlertType, r.Priority, r.DroneID, duration)
		}
	}
	fmt.Printf("[%s] ╚════════════════════════════════════════════════════════════════╝\n\n", b.Name)
}

// -------------replicação de estado via broadcast
//envia uma cópia do TokenPayload atual para todos os peers vivos do anel
func (b *Broker) broadcastState(data TokenPayload) {

	msg := Message{Type: MsgStateUpdate, Payload: data}

	b.peerMu.RLock()
	defer b.peerMu.RUnlock()

	for _, addr := range b.RingAddrs {
		if addr == b.MyAddr {
			continue // pula a si mesmo
		}
		if !b.peerAlive[addr] {
			continue // pula peers mortos
		}

		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			continue
		}
		conn.SetDeadline(time.Now().Add(4 * time.Second))
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			fmt.Printf("[%s] broadcastState: falha ao enviar para %s: %v\n", b.Name, addr, err)
		}
		conn.Close()
		fmt.Printf("[%s] broadcastState → %s (round=%d)\n", b.Name, addr, data.Round)
	}
}


// -------------token ring: escolha de drone disponível com preferência de setor
//consulta o DroneStatus do token (estado global da frota compartilhada)
func (b *Broker) droneDisp(alertSector string) string {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()

	// Candidatos: todos os drones que o token conhece como AVAILABLE.
	type entry struct {
		id     string
		sector string
		hasAddr bool // este broker conhece o endereço físico do drone
	}
	var sameSector, otherSector []entry

	for droneID, globalStatus := range b.tokenData.DroneStatus {
		if globalStatus != "AVAILABLE" {
			continue
		}
		d, known := b.drones[droneID]
		sector := ""
		hasAddr := false
		if known {
			sector = d.Sector
			hasAddr = d.Addr != ""
		}
		e := entry{droneID, sector, hasAddr}
		if sector == alertSector {
			sameSector = append(sameSector, e)
		} else {
			otherSector = append(otherSector, e)
		}
	}

	// Ordena cada grupo: drones com endereço conhecido primeiro, depois por ID.
	rankEntries := func(es []entry) {
		sort.Slice(es, func(i, j int) bool {
			if es[i].hasAddr != es[j].hasAddr {
				return es[i].hasAddr // prefere drone cujo addr é conhecido
			}
			return es[i].id < es[j].id
		})
	}
	rankEntries(sameSector)
	rankEntries(otherSector)

	for _, e := range sameSector {
		return e.id
	}
	for _, e := range otherSector {
		return e.id
	}
	return ""
}

// -------------despacho TCP direto ao drone
//após reservar o drone no token, o broker abre uma conexão TCP com o drone e envia enviaDrone com o ID da missão.
func (b *Broker) enviaDrone(droneID string, alert AlertPayload) bool {
	b.dronesMu.Lock()
	d, ok := b.drones[droneID]
	addr := ""
	if ok {
		addr = d.Addr
	}
	b.dronesMu.Unlock()

	if addr == "" {
		fmt.Printf("[%s] Envia Drone: drone %s sem endereço registrado — não é possível enviar ordem\n",
			b.Name, droneID)
		return false
	}

	msg := Message{
		Type: MsgDroneDispatch,
		Payload: DispatchPayload{
			DroneID:   droneID,
			MissionID: alert.AlertID,
			Sector:    alert.Sector,
			AlertType: alert.AlertType,
			Priority:  alert.Priority,
			Timestamp: time.Now(),
		},
	}

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("[%s] enviaDrone: falha ao conectar em drone %s (%s): %v\n",
			b.Name, droneID, addr, err)
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		fmt.Printf("[%s] enviaDrone: falha ao enviar ordem para drone %s: %v\n",
			b.Name, droneID, err)
		return false
	}

	fmt.Printf("[%s] DISPATCH → drone %s | missão=%s | setor=%s | prioridade=%d\n",
		b.Name, droneID, alert.AlertID, alert.Sector, alert.Priority)
	return true
}

// -------------token ring: limpeza de atribuições antigas
// Remove do mapa assigned os alertas cujo drone já voltou para AVAILABLE.
func (b *Broker) cleanAssigned() {
	for alertID, droneID := range b.tokenData.Assigned {
		status, ok := b.tokenData.DroneStatus[droneID]
		if !ok {
			continue
		}
		if status == "AVAILABLE" {
			// Só limpa se o alerta não está esperando redespacho
			alreadyPending := false
			for _, p := range b.tokenData.PendingAlerts {
				if p.AlertID == alertID {
					alreadyPending = true
					break
				}
			}
			if !alreadyPending {
				delete(b.tokenData.Assigned, alertID)
			}
		}
	}
}

// -------------token ring: passagem do token com bypass de nó morto
// Percorre o anel a partir do próximo nó em relação a este broker
func (b *Broker) passToken(data TokenPayload) {
	myIdx := -1
	for i, addr := range b.RingAddrs {
		if addr == b.MyAddr {
			myIdx = i
			break
		}
	}
	if myIdx < 0 {
		fmt.Printf("[%s] endereço próprio não encontrado no anel!\n", b.Name)
		return
	}

	n := len(b.RingAddrs)
	for step := 1; step < n; step++ {
		nextIdx := (myIdx + step) % n
		nextAddr := b.RingAddrs[nextIdx]

		if nextAddr == b.MyAddr {
			continue // não se enviar para si mesmo
		}

		// Verifica liveness do candidato
		b.peerMu.RLock()
		alive := b.peerAlive[nextAddr]
		b.peerMu.RUnlock()

		if !alive {
			fmt.Printf("[%s] bypass: nó %s morto — tentando próximo\n", b.Name, nextAddr)
			continue
		}

		// Tenta enviar o token
		if b.sendToken(nextAddr, data) {
			fmt.Printf("[%s] token passado para %s (round=%d)\n", b.Name, nextAddr, data.Round)
			return
		}

		// Falha no envio: marca como morto e tenta o seguinte
		fmt.Printf("[%s] falha ao enviar token para %s — bypass\n", b.Name, nextAddr)
		b.peerMu.Lock()
		b.peerAlive[nextAddr] = false
		b.peerMu.Unlock()
	}

	//este broker pega o token se todos os outros estiverem mortos
	fmt.Printf("[%s] único broker vivo — retendo o token\n", b.Name)
	b.tokenMu.Lock()
	b.hasToken = true
	b.lastTokenSeen = time.Now()
	b.tokenMu.Unlock()
	time.Sleep(2 * time.Second)
	go b.tokenLoop()
}

// -------------token ring: envio TCP do token
func (b *Broker) sendToken(addr string, data TokenPayload) bool {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	msg := Message{Type: MsgToken, Payload: data}
	return json.NewEncoder(conn).Encode(msg) == nil
}

// -------------token ring: guarda-tempo para regeneração de token perdido
// se o token não passar por este broker em tempo razoável, ele requisita aos peers que o regenerem
func (b *Broker) tokenTimeoutGuard() {
	// Janela inicial para o ambiente Docker estabilizar os pings
	time.Sleep(5 * time.Second)

	// Tolerância dinâmica: (Quantidade de nós * 3 segundos) + 5 segundos de margem.
	tolerancia := time.Duration(len(b.RingAddrs)*3+5) * time.Second

	for {
		// Mantém o uso de CPU em 0%
		time.Sleep(1 * time.Second)

		b.tokenMu.Lock()
		euTenhoToken := b.hasToken
		ultimoMomentoVisto := b.lastTokenSeen
		b.tokenMu.Unlock()

		// Se este broker está com o token, não há o que recuperar.
		if euTenhoToken {
			continue
		}

		// Se o token passou por aqui recentemente, continua esperando
		if time.Since(ultimoMomentoVisto) < tolerancia {
			continue
		}

		fmt.Printf("[%s] !!! TIMEOUT DETECTADO: %v sem sinal do Token !!!\n", 
			b.Name, time.Since(ultimoMomentoVisto).Round(time.Second))

		// Descobre a minha posição (índice) no anel configurado
		meuIdx := -1
		for i, addr := range b.RingAddrs {
			if addr == b.MyAddr {
				meuIdx = i
				break
			}
		}

		if meuIdx < 0 {
			fmt.Printf("[%s] Erro crítico: meu endereço %s não está em RING_ADDRS\n", b.Name, b.MyAddr)
			continue
		}

		n := len(b.RingAddrs)
		souOResponsavel := false

		// Vamos olhar para trás no anel para descobrir quem deveria ter nos enviado o token.
		// Procuramos o primeiro nó anterior que está VIVO.
		for step := 1; step < n; step++ {
			// Subtrai 'step' para andar para trás no anel de forma circular
			idxAnterior := (meuIdx - step + n) % n
			addrAnterior := b.RingAddrs[idxAnterior]

			if addrAnterior == b.MyAddr {
				// Se deu a volta completa e ninguém está vivo, eu assumo
				souOResponsavel = true
				break
			}

			b.peerMu.RLock()
			anteriorVivo := b.peerAlive[addrAnterior]
			b.peerMu.RUnlock()

			if anteriorVivo {
				// Encontramos o primeiro vizinho vivo de quem deveríamos receber o token.
				souOResponsavel = false
				fmt.Printf("[%s] O nó anterior ativo é %s. Aguardando ele enviar ou falhar.\n", b.Name, addrAnterior)
				break
			}

			// Se o vizinho anterior imediato (step == 1) está MORTO, a responsabilidade de assumir o controle do anel e regenerar o token passa a ser MINHA!
			if step == 1 {
				fmt.Printf("[%s] Meu predecessor direto %s está MORTO. Eu sou o próximo ativo!\n", b.Name, addrAnterior)
				souOResponsavel = true
				break
			}
		}

		if souOResponsavel {
			fmt.Printf("[%s] REGENERANDO TOKEN POR TOPOLOGIA: Assumindo o controle do anel.\n", b.Name)
			
			b.tokenMu.Lock()
			b.hasToken = true
			b.lastTokenSeen = time.Now()
			
			// Restaura a partir do último StateUpdate recebido por broadcast
			if b.backupTokenData.Round > 0 {
				b.backupTokenData.MissionLog = mergeMissionLogs(
					b.tokenData.MissionLog, b.backupTokenData.MissionLog)
				b.tokenData = b.backupTokenData
				b.tokenData.Round++ 
			} else {
				b.tokenData.Round++
				if b.tokenData.Assigned == nil { b.tokenData.Assigned = make(map[string]string) }
				if b.tokenData.DroneStatus == nil { b.tokenData.DroneStatus = make(map[string]string) }
				if b.tokenData.MissionLog == nil { b.tokenData.MissionLog = make(map[string]*MissionRecord) }
			}
			b.tokenMu.Unlock()

			//Reinicia o loop de processamento do token
			go b.tokenLoop()
			
			//Janela de espera maior para o token circular e não duplicar em nós mais distantes
			time.Sleep(5 * time.Second)
		} else {
			b.tokenMu.Lock()
			b.lastTokenSeen = time.Now()
			b.tokenMu.Unlock()
			
			time.Sleep(3 * time.Second)
		}
	}
}


// -------------detecção de drone morto
// verifica a cada 10s se algum drone parou de enviar heartbeat.
func (b *Broker) watchDroneHeartbeats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		b.dronesMu.Lock()
		for _, d := range b.drones {
			if d.Status == "FAILED" {
				continue
			}
			if time.Since(d.LastSeen) > 30*time.Second { // 3× o intervalo de heartbeat (10s)
				fmt.Printf("[%s] drone %s presumido MORTO (sem heartbeat há %v) | missão=%s\n",
					b.Name, d.DroneID,
					time.Since(d.LastSeen).Round(time.Second),
					d.MissionID)

				//salva a missão antes de zerar: ela precisa ser realocada
				orphanMissionID := d.MissionID
				d.Status = "FAILED"
				d.MissionID = ""

				//se o drone estava em missão, reinsere o alerta na fila com prioridade máxima
				if orphanMissionID != "" {
					orphan := AlertPayload{
						AlertID:   orphanMissionID,
						SensorID:  b.Name,
						AlertType: "ORPHAN_MISSION",
						Timestamp: time.Now(),
						Priority:  3, // crítico: missão já iniciada e interrompida
					}
					b.orphanMu.Lock()
					b.orphanAlerts = append(b.orphanAlerts, orphan)
					b.orphanMu.Unlock()
					fmt.Printf("[%s] missão %s marcada como órfã — será realocada no próximo round\n",
						b.Name, orphanMissionID)
				}
			}
		}
		b.dronesMu.Unlock()
	}
}

// faz ping a cada 8s em cada nó do anel para manter o mapa peerAlive
func (b *Broker) peerHeartbeats() {
	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, addr := range b.RingAddrs {
			if addr == b.MyAddr {
				continue
			}

			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)

			b.peerMu.Lock()
			wasAlive := b.peerAlive[addr]

			if err != nil {
				if wasAlive {
					fmt.Printf("[%s] broker peer %s OFFLINE — será bypassado no anel\n",
						b.Name, addr)
					go b.anunciaCobertura(addr)
				}
				b.peerAlive[addr] = false
			} else {
				ping := Message{
					Type:    MsgPeerPing,
					Payload: map[string]string{"broker": b.Name, "addr": b.MyAddr},
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

// -------------aviso de cobertura de setor
func (b *Broker) anunciaCobertura(deadAddr string) {
	msg := Message{
		Type: MsgCoberturaSector,
		Payload: map[string]string{
			"sector":     deadAddr,
			"covered_by": b.Name,
		},
	}
	b.peerMu.RLock()
	defer b.peerMu.RUnlock()
	for _, addr := range b.RingAddrs {
		if addr == deadAddr || addr == b.MyAddr || !b.peerAlive[addr] {
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