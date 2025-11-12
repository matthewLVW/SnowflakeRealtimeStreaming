# Stock Streaming Pipeline

### Real-time capital markets telemetry — designed, engineered, and productized by a single developer.

I built this project to demonstrate the execution bar I bring to data-intensive teams: taking a loosely defined idea (“stream high-resolution ticks into Snowflake in near real time”) and delivering a production-grade system with clear business narrative, operational guardrails, and a polished control surface.

---

## Headlines

| What matters | Detail |
| --- | --- |
| **Latency** | 10 ms raw bins → 1 s OHLCV bars in <1.5 s end-to-end, including Snowflake persistence |
| **Scale realism** | Handles multi-million tick days via Kafka + sliding windows; same architecture powers capital-markets vendors |
| **Ownership** | Every layer authored here: FastAPI data feed, Kafka producers/processors, Snowflake loaders, dashboard, infra automation, docs |
| **Operational polish** | Single-click Streamlit control room orchestrates Docker, replay, aggregation, and Snowflake health with KPI tiles & alerts |
| **Business framing** | Project story ties to liquidity monitoring, revenue protection, and analyst enablement — not just “yet another demo” |

---

## Why This Project Resonates

- **End-to-end problem solving.** I translated a market-data SLA (fresh, lossless, multi-granularity) into concrete architecture, schemas, contracts, monitoring, and runbooks — no hand-offs required.
- **Systems thinking.** Replay producer, Kafka aggregators, and Snowflake loaders share idempotent IDs and late-event handling, preventing silent divergence between systems of record.
- **Executive-ready storytelling.** README, dashboard copy, and CLI helpers speak in outcome language (“latency budgets”, “micro-batch cost”) so stakeholders instantly grasp value.
- **Pragmatic ops mindset.** Auto-suspend Snowflake warehouse, sliding Kafka retention, env-file driven tooling, and validation scripts keep cloud spend and on-call noise small.
- **User experience focus.** The control panel exposes “start mock API / launch pipeline / tail history” workflows in plain language with large-format controls suitable for demos and non-engineers.

---

## System Overview (60-second tour)

```
Historical ticks → Replay Producer → Kafka (ticks.raw, 10 ms bins)
                                  ↓
                           Aggregator (1 s OHLCV, latency guards)
                                  ↓
                    Streamlit dashboard & Live WebSocket viewer
                                  ↓
                      Snowflake loaders (1-min + 5-min facts)
```

Key artifacts:
- **API & replay services (`api/`, `producers/`).** Serve curated NDJSON ticks or live pulls, with configurable pacing to mimic market bursts.
- **Stream processor (`processors/aggregate_1s.py`).** Deduplicates, tracks lateness, and emits metrics to Kafka and structured logs for audits.
- **Snowflake loaders (`consumers/`).** Support password or key-pair auth, batch deletes/inserts, and build 1-min/5-min rollups with idempotent keys.
- **Operator UI (`dashboard/`).** Streamlit console plus WebSocket viewer for real-time inspection; both pull env overrides automatically.
- **Runbooks & DDL (`snowflake/`, `docs/`).** Deployable role/warehouse setup plus processing strategy docs that read like an internal wiki.

---

## Differentiators & Talking Points

- **Latent-value unlock.** By compressing ingestion/aggregation/warehouse latency to seconds, analysts can monitor liquidity shifts mid-session instead of waiting for end-of-day jobs.
- **Cost-aware architecture.** Micro-batches (5×1-minute bars) keep Snowflake compute in XS auto-suspend; Kafka retention trimmed to 60 minutes to reduce local disk overhead.
- **Testing culture baked in.** Validation scripts rebuild aggregates from log files, preventing regressions before code ever touches Kafka.
- **Security & compliance ready.** RSA key-pair authentication, env-scoped secrets, and deterministic bar identifiers mirror enterprise standards.
- **Demo-friendly storytelling.** Makefile + Streamlit allow me to stand up the entire experience live in interviews, highlighting product sense and technical depth simultaneously.

---

## Tech Stack Highlights

| Layer | Tools |
| --- | --- |
| Data transport | Kafka, kafka-python, idempotent replay producer |
| Compute | Python 3.11, FastAPI, Streamlit, cron-style loaders |
| Storage & analytics | Snowflake (RAW, CORE, MART schemas, stage + view strategy) |
| Observability | Structured JSON logs, Streamlit KPI pills, CLI history tracker |
| Tooling & ops | Make, Docker Compose, `.env` templating, validation scripts |

---

## Execution Notes for Interviewers

- **Demo path.** `make pipeline` starts mock API, replay, aggregator, and loader. The Streamlit control panel provides a single pane to start/stop services, inspect KPIs, and run health checks.
- **Dashboard narratives.** The chart view shows candlesticks + latency badges; WebSocket viewer demonstrates how I think about UX responsiveness without page refreshes.
- **Data contracts.** JSON schemas in `docs/contracts/` plus Snowflake DDL show how I enforce producer–consumer compatibility — a common enterprise pain point.
- **Scalability discussions.** I can speak to how this foundation would extend to ksqlDB, Flink, Delta Lake, or cloud-native queues, and what it would take to harden for prod.

---

## Want to Talk?

I’m excited to partner with teams modernizing data platforms, accelerating analytics latency, or building real-time products. This repository is a proof-point of how I combine systems architecture, product thinking, and crisp communication. Reach out if you’d like a live walkthrough or to discuss how similar patterns could impact your organization.



