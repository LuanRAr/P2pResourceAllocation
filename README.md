# 🌐 Ormuz: Sistema de Vigilância IoT Distribuído

🚀 Este projeto foi desenvolvido como solução para o **Problema 2 da disciplina TEC502**.\
Evolução do sistema anterior, agora com **múltiplos brokers distribuídos** em anel (Token Ring), **drones autônomos** como atuadores e tolerância a falhas em todos os níveis.

---

## 🧠 Conceito da Solução

O sistema monitora quatro setores geográficos (Norte, Sul, Leste, Oeste) com sensores que disparam alertas. Brokers coordenam entre si usando um **Token Ring** para garantir que cada alerta seja atribuído a exatamente um drone, sem duplicação e sem ponto único de falha.

```
Sensores  →  Broker (anel TCP)  →  Drones
   ↑               ↕                 ↓
alertas     Token Ring + State     missões
             Update broadcast      concluídas
```

### 📡 Sensores (TCP)
- Geram alertas com valor numérico (70–100)
- Calculam prioridade automaticamente (1=médio, 2=alto, 3=crítico)
- Enviam via TCP com **fallback** para qualquer broker da lista
- Só emitem alertas para valores acima de 70

### 🔁 Broker — Token Ring
- Quatro brokers em anel: Norte, Sul, Leste, Oeste
- Somente o **detentor do token** atribui drones a alertas
- Token carrega o estado global: fila de alertas, status dos drones, log de missões
- **StateUpdate broadcast** replica o estado para todos os peers a cada round
- Token perdido → regeneração automática por eleição topológica (predecessor imediato morto = assumo)

### 🤖 Drones (TCP bidirecional)
- Registram-se em **todos** os brokers ao iniciar
- Enviam heartbeat a cada 10s para todos os brokers
- Recebem ordens de missão (`DRONE_DISPATCH`) via TCP reverso (broker → drone)
- Reportam conclusão de missão (`DRONE_DONE`) com fallback de broker
- Drone morto: missão vira **órfã** e é reinserida na fila com prioridade 3 (crítico)

---

## ⚙️ Tecnologias Utilizadas

- 🐹 **Go 1.22** — toda a lógica de brokers, sensores e drones
- 🌐 **net** — conexões TCP puras (sem framework)
- 📦 **encoding/json** — serialização de mensagens
- 🔄 **sync** (Mutex / RWMutex) — proteção de estado concorrente
- 🐳 **Docker** — build multi-stage e orquestração via Compose

---

## 📁 Estrutura do Projeto

```
ormuz/
├── broker/
│   └── broker.go          # Broker com Token Ring, fila de alertas e despacho de drones
├── sensor/
│   └── sensor.go          # Sensor virtual com geração de alertas e fallback TCP
├── drone/
│   └── drone.go           # Drone com servidor TCP, heartbeat e loop de missão
├── go.mod                 # Módulo Go (ormuz, go 1.22)
├── dockerfile             # Build multi-stage: builder Alpine + runtime mínimo
└── docker-compose.yaml    # 4 brokers + 8 sensores + 8 drones
```

---

## 🔌 Portas e Protocolo

| Componente | Porta     | Protocolo | Direção              |
|------------|-----------|-----------|----------------------|
| Broker     | `5000`    | TCP       | sensores / drones / peers → broker |
| Drone      | `6000–6001` | TCP    | broker → drone (DRONE_DISPATCH) |

Todas as mensagens trafegam como JSON delimitado por newline (`json.Encoder`).

---

## 📨 Tipos de Mensagem

| Tipo               | Remetente  | Destinatário | Descrição                          |
|--------------------|------------|--------------|------------------------------------|
| `ALERT`            | Sensor     | Broker       | Alerta com prioridade e setor      |
| `DRONE_REGISTER`   | Drone      | Broker(s)    | Cadastro inicial com endereço TCP  |
| `DRONE_HEARTBEAT`  | Drone      | Broker(s)    | Sinal de vida com status atual     |
| `DRONE_DONE`       | Drone      | Broker       | Missão concluída                   |
| `DRONE_DISPATCH`   | Broker     | Drone        | Ordem de missão                    |
| `TOKEN`            | Broker     | Broker       | Passagem do token com estado global|
| `STATE_UPDATE`     | Broker     | Broker(s)    | Réplica de estado após cada round  |
| `PEER_PING`        | Broker     | Broker       | Heartbeat entre brokers            |
| `Cobertura_SECTOR` | Broker     | Broker(s)    | Aviso de cobertura de setor morto  |

---

## 🚀 Como Executar

### Pré-requisitos
- Docker ≥ 24 e Docker Compose V2

### Subir tudo de uma vez

```bash
docker compose up --build
```

Isso inicializa:
- **4 brokers** (Norte, Sul, Leste, Oeste) em `172.16.201.7/8/11/12:5000`
- **8 sensores** (Radar e Naval por setor)
- **8 drones** (dois por setor)

### Parar e limpar

```bash
docker compose down
```

---

## ⚙️ Variáveis de Ambiente

### Broker

| Variável      | Exemplo                                              | Descrição                        |
|---------------|------------------------------------------------------|----------------------------------|
| `SECTOR_NAME` | `Norte`                                              | Nome do setor                    |
| `PORT`        | `5000`                                               | Porta TCP de escuta              |
| `MY_IP`       | `172.16.201.7`                                       | IP próprio (para o ping)         |
| `RING_ADDRS`  | `172.16.201.7:5000,...`                              | Lista completa do anel           |

### Sensor

| Variável      | Exemplo                                              | Descrição                        |
|---------------|------------------------------------------------------|----------------------------------|
| `SECTOR_NAME` | `Norte`                                              | Setor de origem dos alertas      |
| `SENSOR_TYPE` | `Radar` ou `Naval`                                   | Tipo do sensor                   |
| `BROKER_LIST` | `172.16.201.7:5000,172.16.201.8:5000,...`            | Brokers (fallback em ordem)      |

### Drone

| Variável      | Exemplo                                              | Descrição                        |
|---------------|------------------------------------------------------|----------------------------------|
| `SECTOR_NAME` | `Norte`                                              | Setor base do drone              |
| `DRONE_ID`    | `DRONE-NORTE-A`                                      | Prefixo do ID                    |
| `DRONE_PORT`  | `6000`                                               | Porta TCP para receber despachos |
| `MY_IP`       | `172.16.201.7`                                       | IP próprio (anunciado ao broker) |
| `BROKER_LIST` | `172.16.201.7:5000,...`                              | Brokers para registro e heartbeat|

---

## 🔍 Funcionalidades

### Sensores
- ✅ Geração aleatória de alertas com intervalos de 5–15 s
- ✅ Filtragem: apenas valores > 70 geram alertas
- ✅ Cálculo automático de prioridade (1/2/3)
- ✅ IDs únicos de alerta (pool de 0–100, embaralhado)
- ✅ Fallback para brokers alternativos em caso de falha

### Brokers (Token Ring)
- ✅ Token Ring TCP com bypass automático de nós mortos
- ✅ Fila de alertas global ordenada por prioridade + timestamp
- ✅ Despacho de drone preferencial por setor geográfico
- ✅ StateUpdate broadcast após cada round (replicação de estado)
- ✅ Regeneração de token por eleição topológica (sem coordenador fixo)
- ✅ Missões órfãs: drone morto em missão → reinsere alerta com prioridade 3
- ✅ Log histórico imutável de missões (PENDENTE → DESPACHADA → CONCLUÍDA/FALHA/ÓRFÃ)
- ✅ Heartbeat entre peers a cada 8 s com detecção de reconexão

### Drones
- ✅ Registro simultâneo em todos os brokers no boot
- ✅ Servidor TCP reverso para receber `DRONE_DISPATCH`
- ✅ Heartbeat periódico (10 s) para todos os brokers
- ✅ Execução de missão com duração aleatória (10–20 s)
- ✅ Reporte de conclusão com fallback de broker
- ✅ Re-registro automático se todos os brokers estiverem inacessíveis após missão

---

## 🏗️ Ciclo de Vida de uma Missão

```
Sensor gera alerta
      ↓
Broker recebe → armazena em localAlerts
      ↓
Token chega neste broker
      ↓
Alerta entra na fila global (ordenada por prioridade)
      ↓
Broker escolhe drone disponível (preferência: mesmo setor)
      ↓
DRONE_DISPATCH → drone via TCP direto
      ↓
Drone executa missão (10–20 s)
      ↓
DRONE_DONE → broker atualiza log: CONCLUÍDA
```

---

## 🛡️ Tolerância a Falhas

| Falha                          | Comportamento                                           |
|--------------------------------|---------------------------------------------------------|
| Broker cai                     | Token bypassado; peers detectam via ping (8 s)          |
| Token perdido                  | Regeneração automática pelo predecessor imediato ativo  |
| Sensor não alcança broker      | Fallback sequencial para próximo broker da lista        |
| Drone cai em missão            | Missão marcada ÓRFÃ; reinserida na fila com prioridade 3|
| Drone cai ocioso               | Marcado FAILED após 30 s sem heartbeat; ignorado        |
| Todos os brokers mortos        | Broker remanescente retém o token e opera sozinho       |

---

## 💡 Resumo

🔁 **Token Ring** — coordenação distribuída sem líder fixo\
⚡ **TCP puro** — sem dependência de middleware externo\
🛡️ **Tolerância a falhas** em todos os níveis (sensor, broker, drone)\
📊 **Log de missões** com ciclo de vida completo e rastreável\
🐳 **Docker multi-stage** — imagem de runtime mínima (Alpine)
