# IoT First — Sistema de Streaming para Monitoramento de Sensores IoT

## Descrição do Case

Este projeto implementa um **sistema completo de ingestão, processamento, validação e armazenamento de dados de sensores IoT** em um ambiente de Big Data simulado.

A solução utiliza:

- **Kafka** → ingestão em tempo real  
- **Python Producer** → simulação de sensores IoT com dados sintéticos  
- **Spark Structured Streaming** → consumo, validação, enriquecimento e agregação  
- **PostgreSQL** → persistência confiável dos dados  
- **Soda Core** → monitoramento de qualidade de dados  
- **Pytest** → testes robustos  
- **Docker Compose** → reprodutibilidade em qualquer ambiente  

---

# 1. Arquitetura de Solução e Arquitetura Técnica

## Visão Geral da Arquitetura

```
           ┌──────────────────────┐
           │   Sensor Simulator   │
           │  kafka_producer.py   │
           └────────────┬─────────┘
                        │ Envia JSON
                        ▼
              ┌───────────────────┐
              │      Kafka        │
              └─────────┬─────────┘
                        │ Stream
                        ▼
      ┌────────────────────────────────────┐
      │        Spark Structured Streaming  │
      │   kafka_consumer_spark.py          │
      │                                    │
      │  - Validação (válidos / DLQ)       │
      │  - Normalização                    │
      │  - Alertas                         │
      │  - Agregações 1 minuto             │
      └───────┬───────────────┬────────────┘
              │               │
              ▼               ▼
   ┌────────────────┐  ┌───────────────────┐
   │ sensor_readings │  │ sensor_errors(DLQ)│
   └────────────────┘  └───────────────────┘
              │
              ▼
   ┌──────────────────────┐
   │ sensor_aggregations │
   └──────────────────────┘
```

---

## Tecnologias Utilizadas

- **Apache Kafka** – ingestão de eventos IoT em tempo real  
- **Apache Zookeeper** – coordenação do cluster Kafka  
- **Spark Structured Streaming** – processamento contínuo, agregações e DQ  
- **PostgreSQL 15** – armazenamento persistente + views analíticas  
- **Soda Core** – verificações de qualidade de dados  
- **Pytest + Spark local** – suíte de testes determinística  
- **Docker Compose** – orquestração e reprodutibilidade  
- **Python 3.10** – Producer, Consumer e ferramentas auxiliares 

---

## Decisões de Design

1. **Kafka** como barramento de ingestão desacoplado  
2. **Spark Structured Streaming** com checkpointing & watermark  
3. **DLQ real** (`sensor_errors`) incluindo *raw_value + metadados Kafka*  
4. **Normalização e calculo de alertas** nativamente no Consumer  
5. **Agregações por janela de 1 minuto** com métricas completas  
6. **metadata YAML + Soda Core** para DQ governado  
7. **Código somente leitura dentro dos containers**  
8. **Testes automáticos executados antes do cluster subir** (dev-first CI local) 

(Devido à unicidade da fonte, optou-se por não implementar uma arquitetura medalhão neste caso.)

---

# 1.1. Orquestração e Comportamento do Docker Compose

Este projeto tenta simular os passos de um sistema de deploy seguro, a fim de garantir que todos os componentes estejam saudáveis antes de iniciar o processamento.

### ✔ Ordem de inicialização

1. **tests**  
   - Roda automaticamente usando o container `iot-first-tests`  
   - Precisa PASSAR (`service_completed_successfully`) a fim de garantir a produtividade dos demais serviços  

2. **zookeeper → kafka → postgres**  
   - Cada etapa aguarda *healthcheck*  

3. **spark-consumer**  
   - Aguarda:  
     - testes  
     - kafka saudável  
     - postgres saudável  

4. **producer**  
   - Aguarda kafka saudável  

5. **soda-core**  
   - Aguarda postgres saudável  

### Estrutura de rede

```
networks:
  default:
    name: iot_first_net
```

Todos os containers comunicam-se por DNS interno:  
`kafka:9092`, `postgres:5432`, `zookeeper:2181`, etc.

### Persistência

Volumes montados em:

- `volumes/kafka`
- `volumes/postgres`
- `volumes/spark/checkpoints`
- `volumes/spark/logs`
- `volumes/soda/logs`
- `volumes/zookeeper`

(intenção de garantir rasrteabilidade fora do ambiente de execução das imagens docker)

---

# 2. Explicação sobre o Case Desenvolvido (Plano de Implementação)

## 2.1 Producer – Geração & Envio de Dados

Arquivo: `python/kafka_producer.py`

- Utiliza **Faker** para simular sensores: temperatura, umidade e pressão  
- Envia dados JSON continuamente para o Kafka  
- Cada leitura contém:
  - sensor_id
  - sensor_type
  - value (com simulações de falhas: abaixo do minimo, acima do máximo)
  - manufacturer, model, unit
  - min/max
  - status (75% ativo, 25% manutenção)

É enviado um batch de medições a cada **2 segundos**.

---

## 2.2 Consumer – Streaming com Spark

Arquivo: `python/kafka_consumer_spark.py`

### Pipeline:

### 1. Leitura do Kafka  
Com tolerância a falhas, failOnDataLoss=false e timeouts configurados.

### 2. Validação: `split_valid_invalid`  
Regras implementadas:

| Regra | Direção | Resultado DLQ |
|-------|---------|---------------|
| JSON inválido | → DLQ | JSON_PARSE_ERROR |
| status != "active" | → DLQ | INACTIVE_SENSOR |
| value NULL | → DLQ | NULL_VALUE |
| value < min | → DLQ | BELOW_MIN |
| value > max | → DLQ | ABOVE_MAX |
| válido | segue p/ processamento | ---------------|

Além disso, o Consumer preserva:

- topic  
- partition  
- offset  
- kafka_timestamp  

(metadado importante para rastreamento)

---

### 3. Processamento: `process_sensor_data`

Aplica:

- Conversão de timestamps  
- Normalização:  
  `(value - min) / (max - min)`  
- Geração de:
  - high_alert (dentro dos 90-100% percentis) 
  - low_alert (dentro dos 0-10% percentis) 
  - processing_time  

---

### 4. Escrita em PostgreSQL

Tabela `sensor_readings` recebe os dados brutos processados.

### 5. Agregações por janela

Com:

```
window(timestamp, "1 minute")
```

Calcula:

- média  
- mínimo / máximo  
- desvio padrão  
- contagem  
- quantidade de alertas  

Persistidos em `sensor_aggregations`.

(ponto de atenção: dados gerados pela biblioteca faker raramente tenderão a ser agregados, devido à natureza aleatória das chaves)

### 6. DLQ

Erros estruturados são persistidos em `sensor_errors`. Buscou-se implementar, também, a motivação do erro em coluna própria.

---

# 2.3 Banco de Dados

As tabelas criadas no arquivo `db/init.sql` incluem:

- `sensor_readings`
- `sensor_aggregations`
- `sensor_errors`
- Índices para performance
- Views analíticas como:
  - `latest_sensor_status`
  - `hourly_sensor_summary`
  - `dq_error_rate`

Essas views permitem análises imediatas após a ingestão.

---

# 2.4 Qualidade de Dados com Soda

Arquivos YAML localizados em:

```
db/metadata/*.yml
soda/checks/*.yml
```

Os checks incluem:

- validação de schemas
- checagem de nulos
- ranges esperados  
- taxa de erros (via view dq_error_rate)  
- consistência do grain  

---

# 2.5 Testes Automatizados

Tests em: `tests/`

### Abrangência:

### split_valid_invalid  
- separa válidos e inválidos  
- identifica JSON_PARSE_ERROR  
- identifica BELOW_MIN, ABOVE_MAX etc  
- preserva metadados do Kafka  

### process_sensor_data  
- normalização correta  
- high_alert / low_alert corretos  
- apenas válidos seguem ao processamento  

### create_aggregations  
- janelas de 1 minuto  
- métricas calculadas corretamente  
- contagem de alertas coerente  

Os testes usam Spark local (1 executor) para evitar flutuações.

---

# 2.6 Configuração do Ambiente (.env e secrets)

Muito embora os arquivos estejam disponibilizados no repositório, se necessário, crie o arquivo `.env` na raiz:

```
# Kafka
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=iot_sensor_data

# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=iot_db
POSTGRES_USER=iot_user
```

Também, crie o segredo do PostgreSQL:

```
echo "sua_senha_segura_aqui" > secrets/pg_password.txt
```

Esse arquivo é montado em `/run/secrets/pg_password`.

(importante ressaltar que, em ambientes de produção, o uso de ferramentas próprias é recomendado para maior segurança. No presente caso, o uso de Secrets visa apenas simular boas práticas.)

---

# 3. Melhorias e Considerações Finais

## Melhorias Futuras

1. Implementação de dashboard Grafana para acompanhamento em tempo real de métricas  
2. Disponibilização de API REST para consulta das últimas medições, ou mesmo de agregações de longo prazo  
3. Uso de Delta Lake para formatos otimizados  
4. MLflow para modelos de detecção de anomalia  
5. AuthN/AuthZ no Producer e Consumer  
6. Criação de usuários personalizados no Postgres com roles específicas  
7. Monitoramento avançado com alertas a serem enviados por e-mail em caso de falhas críticas
8. Implementação de CI/CD para deploy automático via GitHub Actions ou similar
9. Implementação de arquitetura medalhão para maior resiliência e governança dos dados

---

## Desafios Encontrados

- Implementação de sistema streaming de ponta a ponta
- Sincronização entre Kafka / Spark / Postgres via Docker  
- Parsing robusto de mensagens inválidas  
- Garantir determinismo dos testes PySpark  
- Confiabilidade ao lidar com offsets e DLQ  

---

# Estrutura de Pastas

```
.
├── .gitignore
├── .dockerignore
├── .env
├── docker-compose.yml
├── LICENSE
├── pyproject.toml
├── README.md
├── conf/
│   └── log4j2.properties
├── db/
│   ├── init.sql
│   └── metadata/
│       ├── sensor_aggregations.yml
│       ├── sensor_errors.yml
│       └── sensor_readings.yml
├── docker/
│   ├── Dockerfile.producer
│   ├── Dockerfile.soda
│   ├── Dockerfile.spark-consumer
│   └── Dockerfile.test
├── python/
│   ├── kafka_consumer_spark.py
│   ├── kafka_producer.py
│   └── tools/
│       └── logger_utils.py
├── secrets/
│   └── pg_password.txt
├── soda/
│   ├── soda_configuration.yml
│   └── checks/
│       ├── iot_quality.yml
│       └── iot_sensor_readings.yml
├── tests/
│   ├── conftest.py
│   ├── test_consumer_aggregations.py
│   ├── test_consumer_metadata.py
│   └── test_consumer_records.py
└── volumes/
    ├── kafka/
    │   └── logs/
    ├── postgres/
    ├── soda/
    │   └── logs/
    ├── spark/
    │   ├── checkpoints/
    │   └── logs/
    └── zookeeper/
        ├── data/
        └── log/
```

---

# ▶️ Como Executar

## 1. Clonar o projeto
```bash
git clone https://github.com/renangonzales/iot_first.git
cd iot_first
```

## 2. Subir a stack
```bash
docker compose up --build
```

---

# Conclusão

Este projeto implementa **de ponta a ponta** um pipeline de streaming IoT:

- Simulador de sensores  
- Ingestão em Kafka  
- Processamento contínuo com Spark  
- Validação completa  
- DLQ rastreável  
- Agregações temporais  
- Banco relacional  
- Qualidade de dados (Soda)  
- Testes automatizados  

Tudo containerizado.

