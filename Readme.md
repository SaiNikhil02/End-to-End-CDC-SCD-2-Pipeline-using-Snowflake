Snowflake CDC & SCD Type-2 Pipeline
📌 Overview

This project implements an end-to-end, event-driven CDC (Change Data Capture) pipeline using Apache NiFi, Amazon S3, and Snowflake.
It demonstrates how to ingest data from external systems, apply incremental changes efficiently, and maintain SCD Type-2 history using Snowflake-native features such as Snowpipe, Streams, and Tasks.

The pipeline is designed to be scalable, fault-tolerant, replay-safe, and production-ready.

🏗️ Architecture

High-Level Flow
Apache NiFi (Docker on EC2)
        ↓
      Amazon S3
        ↓
   Snowpipe (Auto-Ingest)
        ↓
    customer_raw (Staging Table)
        ↓ (Snowflake Stream)
     customer (Current State)
        ↓ (Snowflake Stream)
  customer_history (SCD Type-2)

🧰 Tech Stack

Apache NiFi – Data ingestion and flow orchestration

Docker – Containerized NiFi runtime

Amazon EC2 – Compute for NiFi

Amazon S3 – Durable landing zone

Snowflake

Snowpipe (auto-ingest)

Streams (CDC)

Tasks (automation)

MERGE-based incremental processing

SCD Type-2 modeling

📂 Repository Structure
snowflake-cdc-scd2-pipeline/
│
├── README.md
│
├── architecture/
│   └── cdc_pipeline_architecture.png
│
├── nifi/
│   ├── customer_ingestion_flow.xml
│   └── README.md
│
├── snowflake/
│   ├── ddl/
│   │   ├── customer_tables.sql
│   │   ├── streams.sql
│   │   └── file_formats.sql
│   │
│   ├── ingestion/
│   │   ├── snowpipe.sql
│   │   └── copy_history_checks.sql
│   │
│   ├── cdc/
│   │   ├── merge_customer.sql
│   │   └── scd2_customer_history.sql
│   │
│   └── tasks/
│       ├── task_customer_current.sql
│       └── task_customer_history.sql
│
├── sample-data/
│   └── customer_sample.csv
│
└── .gitignore

<b> 🔄 Data Flow Explained </b>
1️⃣ Ingestion (Apache NiFi → S3)

Apache NiFi runs on Dockerized EC2 

Ingests data from source systems

Performs light validation and routing

Writes files to Amazon S3

2️⃣ Auto-Ingest (S3 → Snowflake)

Snowpipe listens for S3 events

Automatically loads data into customer_raw

Ingestion is append-only

3️⃣ CDC for Current State

A Snowflake Stream on customer_raw captures inserts/deletes

A MERGE operation updates the customer table:

INSERT → new records

UPDATE → matched records

DELETE → removals

4️⃣ SCD Type-2 History

A second Stream on customer tracks business-level changes

History logic:

Close old version (end_time, is_current = FALSE)

Insert new version (is_current = TRUE)

Ensures exactly one active row per business key

5️⃣ Automation

Snowflake Tasks:

Triggered only when streams have data

Chained execution ensures correct ordering

No external schedulers required

🧠 Key Design Decisions

Raw tables are append-only (no truncation)

One stream per consumer (best practice)

CDC is explicit, not inferred

History is driven from target table changes

Incremental processing (no full table scans)

Replay-safe and idempotent

🧪 Supported Scenarios

New customer insert

Customer updates (tracked historically)

Customer deletes (history closed)

File replays without duplicate inserts

Multiple changes per key handled safely

🔐 Security & Best Practices

No credentials committed to GitHub

Secrets replaced with placeholders

IAM & Snowflake RBAC enforced

Tasks run with least-privilege roles

▶️ How to Run (High Level)

Deploy NiFi on EC2 (Docker)

Configure NiFi flow → push files to S3

Create Snowflake objects:

Tables

Streams

File formats

Snowpipe

Create and resume Tasks

Monitor via:

SYSTEM$STREAM_HAS_DATA

TASK_HISTORY

COPY_HISTORY

