package main

//-------------broker principal
// Broker do sistema distribuído de monitoramento do Estreito de Ormuz.
// Utiliza Token Ring para garantir exclusão mútua no despacho de drones:
//   - Apenas o broker que detém o token pode despachar drones.
//   - O token circula pelo anel em ordem; se um broker cai, o anel é
//     reconfigurado com bypass automático do nó morto.
//   - O token carrega a fila global de alertas pendentes e o estado dos drones,
//     garantindo consistência distribuída sem ponto único de falha.

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

//-------------tipos de mensagem
type MessageType string

const (
	MsgAlert          MessageType = "ALERT"
	MsgDroneRegister  MessageType = "DRONE_REGISTER"
	MsgDroneHeartbeat MessageType = "DRONE_HEARTBEAT"
	MsgDroneDone      MessageType = "DRONE_DONE"
	MsgPeerPing       MessageType = "PEER_PING"       // broker → broker: sinal de vida
	MsgTakeoverSector MessageType = "TAKEOVER_SECTOR" // broker → broker: aviso de cobertura
	MsgToken          MessageType = "TOKEN"           // token ring: passagem do token
	MsgTokenRequest   MessageType = "TOKEN_REQUEST"   // broker requisita regeneração do token
)

//-------------estruturas de mensagem
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

	// Priority é calculado pelo sensor no momento da emissão do alerta.
	// Quanto MAIOR o valor, maior a urgência de atendimento.
	// Faixa: 1 (baixo) a 3 (crítico), derivado de Value:
	//   Value > 95 → 3 (crítico)
	//   Value > 85 → 2 (alto)
	//   Value > 70 → 1 (médio)  ← limiar de envio do sensor
	Priority int `json:"priority"`
}

//-------------ordenação da fila de alertas por prioridade + timestamp
// Critério composto:
//   1º  Priority DESC  — ocorrência mais crítica primeiro
//   2º  Timestamp ASC  — entre mesma prioridade, quem chegou antes tem precedência
// Implementa sort.Interface para uso com sort.Sort.
type AlertQueue []AlertPayload

func (q AlertQueue) Len() int      { return len(q) }
func (q AlertQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q AlertQueue) Less(i, j int) bool {
	if q[i].Priority != q[j].Priority {
		return q[i].Priority > q[j].Priority // maior prioridade primeiro
	}
	return q[i].Timestamp.Before(q[j].Timestamp) // mais antigo primeiro
}

type DronePayload struct {
	DroneID   string    `json:"drone_id"`
	Sector    string    `json:"sector"`
	Status    string    `json:"status"`
	MissionID string    `json:"mission_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// TokenPayload carrega o estado global compartilhado pelo anel.
// Ao receber o token, o broker atualiza sua visão local e processa
// a fila antes de repassar o token ao próximo nó vivo.
type TokenPayload struct {
	// Fila global de alertas ainda sem drone atribuído
	PendingAlerts []AlertPayload `json:"pending_alerts"`

	// Mapa de alert_id → drone_id para alertas já atribuídos
	Assigned map[string]string `json:"assigned"`

	// Mapa de drone_id → status global ("AVAILABLE" | "IN_MISSION" | "FAILED")
	DroneStatus map[string]string `json:"drone_status"`

	// Número de voltas completas (debug / detecção de token perdido)
	Round uint64 `json:"round"`
}

//-------------estado do drone (local ao broker)
type DroneState struct {
	DroneID   string
	Status    string    // "AVAILABLE" | "IN_MISSION" | "FAILED"
	MissionID string
	LastSeen  time.Time
}

//-------------broker
type Broker struct {
	Name    string
	Port    string
	MyAddr  string   // IP:porta deste broker (para o ping)
	RingAddrs []string // todos os endereços do anel, em ordem

	// Estado local dos drones registrados neste broker
	dronesMu sync.Mutex
	drones   map[string]*DroneState

	// Alertas recebidos localmente enquanto não temos o token
	localAlertsMu sync.Mutex
	localAlerts   []AlertPayload

	// Alertas órfãos: missões cujo drone foi marcado como FAILED.
	// São reinseridos na fila global com prioridade máxima no próximo round.
	orphanMu     sync.Mutex
	orphanAlerts []AlertPayload

	// token ring
	tokenMu   sync.Mutex
	hasToken  bool
	tokenData TokenPayload // última versão conhecida do token

	// Liveness dos peers (indexed by addr)
	peerMu    sync.RWMutex
	peerAlive map[string]bool

	// Canal para acordar o loop do token quando um alerta chega
	alertReady chan struct{}
}

func NewBroker(name, port, myAddr string, ring []string) *Broker {
	b := &Broker{
		Name:       name,
		Port:       port,
		MyAddr:     myAddr,
		RingAddrs:  ring,
		drones:     make(map[string]*DroneState),
		peerAlive:  make(map[string]bool),
		alertReady: make(chan struct{}, 64),
		tokenData: TokenPayload{
			PendingAlerts: []AlertPayload{},
			Assigned:      make(map[string]string),
			DroneStatus:   make(map[string]string),
		},
	}
	// Inicialmente considera todos os peers offline até o ping confirmar
	for _, addr := range ring {
		if addr != myAddr {
			b.peerAlive[addr] = false
		}
	}
	return b
}

//-------------main
func main() {
	name     := os.Getenv("SECTOR_NAME")
	port     := os.Getenv("PORT")
	myIP     := os.Getenv("MY_IP")
	ringRaw  := os.Getenv("RING_ADDRS") // anel completo em ordem: "IP1:5000,IP2:5000,IP3:5000"

	if port == "" { port = "5000" }
	myAddr := fmt.Sprintf("%s:%s", myIP, port)

	var ring []string
	for _, addr := range strings.Split(ringRaw, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			ring = append(ring, addr)
		}
	}

	b := NewBroker(name, port, myAddr, ring)
	fmt.Printf("=== Broker [%s] | addr=%s | anel=%v ===\n", name, myAddr, ring)

	// Goroutines auxiliares
	go b.watchDroneHeartbeats() // detecta drones mortos
	go b.watchPeerHeartbeats()  // detecta brokers vizinhos mortos + envia ping
	go b.tokenTimeoutGuard()    // regenera token se ficar perdido

	// Apenas o primeiro nó do anel inicia com o token
	if len(ring) > 0 && ring[0] == myAddr {
		fmt.Printf("[%s] sou o nó inicial — gerando token\n", name)
		b.tokenMu.Lock()
		b.hasToken = true
		b.tokenMu.Unlock()
		go b.tokenLoop()
	}

	// Listener TCP principal
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

//-------------recepção de mensagens
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

	case MsgTakeoverSector:
		var p map[string]string
		if json.Unmarshal(raw["payload"], &p) == nil {
			fmt.Printf("[%s] broker %s assumiu cobertura do setor %s\n",
				b.Name, p["covered_by"], p["sector"])
		}

	//-------------token ring: recebimento do token
	case MsgToken:
		var p TokenPayload
		if json.Unmarshal(raw["payload"], &p) == nil {
			b.receiveToken(p)
		}

	//-------------token ring: solicitação de regeneração
	case MsgTokenRequest:
		b.tokenMu.Lock()
		alreadyHas := b.hasToken
		b.tokenMu.Unlock()
		if !alreadyHas {
			fmt.Printf("[%s] recebeu TOKEN_REQUEST — regenerando token perdido\n", b.Name)
			b.tokenMu.Lock()
			b.hasToken = true
			b.tokenMu.Unlock()
			go b.tokenLoop()
		}
	}
}

//-------------alerta: enfileira localmente e acorda o loop do token
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

//-------------gestão de drones
func (b *Broker) handleDroneRegister(p DronePayload) {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()

	if existing, ok := b.drones[p.DroneID]; ok {
		existing.Status   = "AVAILABLE"
		existing.LastSeen = time.Now()
		fmt.Printf("[%s] drone %s re-registrado\n", b.Name, p.DroneID)
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
		// Drone adotado (broker original caiu)
		b.drones[p.DroneID] = &DroneState{
			DroneID:   p.DroneID,
			Status:    p.Status,
			MissionID: p.MissionID,
			LastSeen:  time.Now(),
		}
		fmt.Printf("[%s] drone %s adotado via heartbeat\n", b.Name, p.DroneID)
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

//-------------token ring: recebimento
// O broker atualiza seu estado local com as informações do token,
// absorve os alertas locais pendentes na fila global e inicia
// o processamento (despacho de drones).
func (b *Broker) receiveToken(p TokenPayload) {
	b.tokenMu.Lock()
	defer b.tokenMu.Unlock()

	if b.hasToken {
		// Duplicata (pode acontecer durante regeneração): descarta
		fmt.Printf("[%s] token duplicado recebido — descartando\n", b.Name)
		return
	}

	fmt.Printf("[%s] token recebido | round=%d | pendentes=%d\n",
		b.Name, p.Round, len(p.PendingAlerts))

	b.hasToken  = true
	b.tokenData = p

	go b.tokenLoop()
}

//-------------token ring: loop de processamento
// Executado enquanto este broker detiver o token.
// Ordem das operações:
//   1. Absorve alertas órfãos (drones FAILED) com prioridade máxima
//   2. Absorve alertas locais novos
//   3. Sincroniza estado global ↔ local dos drones
//   4. Recoloca na fila os alertas cujo drone falhou durante a missão
//   5. Ordena a fila por Priority DESC + Timestamp ASC
//   6. Tenta despachar drones para cada alerta pendente
//   7. Passa o token ao próximo nó vivo
func (b *Broker) tokenLoop() {
	// Pequena espera para absorver alertas que chegam em rajada
	time.Sleep(200 * time.Millisecond)

	b.tokenMu.Lock()

	// 1. Absorve alertas órfãos com prioridade máxima (drone caiu em missão)
	b.orphanMu.Lock()
	for _, orphan := range b.orphanAlerts {
		if _, already := b.tokenData.Assigned[orphan.AlertID]; !already {
			// Remove a atribuição antiga (o drone morreu) e reinsere na fila
			delete(b.tokenData.Assigned, orphan.AlertID)
			b.tokenData.PendingAlerts = append(b.tokenData.PendingAlerts, orphan)
			fmt.Printf("[%s] alerta órfão %s reinserido na fila (prioridade=%d)\n",
				b.Name, orphan.AlertID, orphan.Priority)
		}
	}
	b.orphanAlerts = nil
	b.orphanMu.Unlock()

	// 2. Absorve alertas locais novos na fila global do token
	b.localAlertsMu.Lock()
	for _, a := range b.localAlerts {
		if _, already := b.tokenData.Assigned[a.AlertID]; !already {
			b.tokenData.PendingAlerts = append(b.tokenData.PendingAlerts, a)
		}
	}
	b.localAlerts = nil
	b.localAlertsMu.Unlock()

	// 3. Sincroniza estado global dos drones com o estado local
	b.dronesMu.Lock()
	for droneID, d := range b.drones {
		b.tokenData.DroneStatus[droneID] = d.Status
	}
	// Aplica mudanças globais (ex.: drones de outros brokers liberados)
	for droneID, globalStatus := range b.tokenData.DroneStatus {
		if local, ok := b.drones[droneID]; ok {
			if local.Status != "FAILED" {
				local.Status = globalStatus
			}
		}
	}
	b.dronesMu.Unlock()

	// 4. Recoloca na fila os alertas cujo drone aparece como FAILED no token
	// Isso cobre o caso em que o drone falhou em um broker diferente do atual.
	for alertID, droneID := range b.tokenData.Assigned {
		if status, ok := b.tokenData.DroneStatus[droneID]; ok && status == "FAILED" {
			// Reconstrói um AlertPayload mínimo para reinserção.
			// O Timestamp é mantido original (se já estiver na fila) ou Now().
			alreadyPending := false
			for _, p := range b.tokenData.PendingAlerts {
				if p.AlertID == alertID {
					alreadyPending = true
					break
				}
			}
			if !alreadyPending {
				recovered := AlertPayload{
					AlertID:   alertID,
					SensorID:  droneID, // melhor esforço: reusa o campo para rastreio
					AlertType: "RECOVERED",
					Timestamp: time.Now(),
					Priority:  3, // prioridade máxima: missão interrompida
				}
				b.tokenData.PendingAlerts = append(b.tokenData.PendingAlerts, recovered)
				delete(b.tokenData.Assigned, alertID)
				fmt.Printf("[%s] TOKEN → alerta %s recuperado (drone %s falhou) — reinserido com prioridade máxima\n",
					b.Name, alertID, droneID)
			}
		}
	}

	// 5. Ordena a fila: Priority DESC, Timestamp ASC
	sort.Sort(AlertQueue(b.tokenData.PendingAlerts))

	// 6. Tenta despachar drones para os alertas pendentes (em ordem de prioridade)
	var stillPending []AlertPayload
	for _, alert := range b.tokenData.PendingAlerts {
		if _, already := b.tokenData.Assigned[alert.AlertID]; already {
			continue // já atribuído em round anterior
		}

		droneID := b.pickAvailableDrone()
		if droneID == "" {
			stillPending = append(stillPending, alert)
			fmt.Printf("[%s] sem drone disponível para alerta %s (prioridade=%d) — mantendo na fila\n",
				b.Name, alert.AlertID, alert.Priority)
			continue
		}

		// Marca o drone como em missão localmente e no token
		b.dronesMu.Lock()
		if d, ok := b.drones[droneID]; ok {
			d.Status    = "IN_MISSION"
			d.MissionID = alert.AlertID
		}
		b.dronesMu.Unlock()

		b.tokenData.Assigned[alert.AlertID] = droneID
		b.tokenData.DroneStatus[droneID]    = "IN_MISSION"

		fmt.Printf("[%s] TOKEN → drone %s despachado para alerta %s (prioridade=%d)\n",
			b.Name, droneID, alert.AlertID, alert.Priority)
	}
	b.tokenData.PendingAlerts = stillPending
	b.tokenData.Round++

	// 7. Limpa atribuições de missões já concluídas
	b.cleanAssigned()

	// Libera o token antes de passar adiante
	b.hasToken = false
	tokenSnapshot := b.tokenData
	b.tokenMu.Unlock()

	// 8. Passa o token ao próximo nó vivo do anel
	b.passToken(tokenSnapshot)
}

//-------------token ring: escolha de drone disponível
func (b *Broker) pickAvailableDrone() string {
	b.dronesMu.Lock()
	defer b.dronesMu.Unlock()
	for _, d := range b.drones {
		if d.Status == "AVAILABLE" {
			return d.DroneID
		}
	}
	return ""
}

//-------------token ring: limpeza de atribuições antigas
// Remove do mapa assigned os alertas cujo drone já voltou para AVAILABLE,
// evitando crescimento ilimitado do payload do token.
func (b *Broker) cleanAssigned() {
	for alertID, droneID := range b.tokenData.Assigned {
		if status, ok := b.tokenData.DroneStatus[droneID]; ok {
			if status == "AVAILABLE" {
				delete(b.tokenData.Assigned, alertID)
			}
		}
	}
}

//-------------token ring: passagem do token com bypass de nó morto
// Percorre o anel a partir do próximo nó em relação a este broker.
// Se o sucessor estiver morto, salta para o seguinte (bypass),
// implementando reconfiguração dinâmica do anel.
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
		nextIdx  := (myIdx + step) % n
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

	// Todos os outros nós estão mortos — este broker retém o token
	fmt.Printf("[%s] único broker vivo — retendo o token\n", b.Name)
	b.tokenMu.Lock()
	b.hasToken = true
	b.tokenMu.Unlock()
	go b.tokenLoop()
}

//-------------token ring: envio TCP do token
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

//-------------token ring: guarda-tempo para regeneração de token perdido
// Se o token não passar por este broker em tempo razoável,
// ele requisita aos peers que o regenerem, garantindo que o anel
// não trave permanentemente após falhas em cascata.
func (b *Broker) tokenTimeoutGuard() {
	// Tolerância: 3 × número de nós × 2 segundos por nó + folga
	checkInterval := time.Duration(len(b.RingAddrs)*2+10) * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		b.tokenMu.Lock()
		has := b.hasToken
		b.tokenMu.Unlock()

		if has {
			continue // tudo bem, tenho o token
		}

		// Verifica se há algum broker vivo que possa ter o token
		b.peerMu.RLock()
		anyAlive := false
		for _, alive := range b.peerAlive {
			if alive {
				anyAlive = true
				break
			}
		}
		b.peerMu.RUnlock()

		if !anyAlive {
			// Nenhum peer vivo: este broker assume como único nó e regenera
			fmt.Printf("[%s] timeout do token e sem peers — regenerando token\n", b.Name)
			b.tokenMu.Lock()
			b.hasToken = true
			b.tokenMu.Unlock()
			go b.tokenLoop()
		} else {
			// Solicita regeneração ao anel
			fmt.Printf("[%s] timeout do token — solicitando regeneração\n", b.Name)
			b.broadcastTokenRequest()
		}
	}
}

// broadcastTokenRequest envia MsgTokenRequest a todos os peers vivos.
// O primeiro que receber e não tiver o token o regenera.
func (b *Broker) broadcastTokenRequest() {
	msg := Message{Type: MsgTokenRequest, Payload: map[string]string{"from": b.MyAddr}}
	b.peerMu.RLock()
	defer b.peerMu.RUnlock()
	for addr, alive := range b.peerAlive {
		if !alive || addr == b.MyAddr {
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

//-------------detecção de drone morto
// Verifica a cada 10s se algum drone parou de enviar heartbeat.
// Quando um drone é marcado como FAILED durante uma missão ativa,
// o AlertID da missão é enfileirado como alerta órfão com prioridade
// máxima (3) para ser redistribuído no próximo round do token.
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

				// Salva a missão antes de zerar: ela precisa ser realocada
				orphanMissionID := d.MissionID
				d.Status    = "FAILED"
				d.MissionID = ""

				// Se o drone estava em missão, reinsere o alerta na fila
				// com prioridade máxima para ser atendido o quanto antes.
				qhanMissionID != "" {
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

//-------------detecção de broker morto (ping TCP periódico)
// Faz ping a cada 8s em cada nó do anel para manter o mapa peerAlive
// atualizado. Esse mapa é consultado pelo passToken para bypass.
func (b *Broker) watchPeerHeartbeats() {
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
					go b.announceTakeover(addr)
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

//-------------aviso de cobertura de setor
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