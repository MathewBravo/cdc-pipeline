# CDC Pipeline

A change data capture system that streams PostgreSQL database changes to Kafka in real-time. Built to learn database internals and distributed systems patterns.

## What It Does

Captures database changes from PostgreSQL's logical replication stream and publishes them to Kafka. Handles 2,500 events per second with sub-2 second end-to-end latency.

The pipeline reads from PostgreSQL's WAL, applies configurable transformations, and writes to Kafka with ordering guarantees. Supports filtering, routing, and PII compliance (hashing, masking, redaction) via YAML configuration.

## Architecture

- **PostgreSQL Source**: Connects to logical replication slot, decodes WAL changes
- **Transformation Layer**: YAML-driven rules for operation filtering, table routing, field-level transformations
- **Kafka Sink**: Deterministic partitioning by composite primary keys, configurable batching and compression
- **Checkpointing**: LSN-based exactly-once semantics with atomic file writes

Built with Go using channels and goroutines for concurrent processing. Graceful shutdown via context cancellation and WaitGroups.

## Technical Details

- Kafka producer: franz-go with snappy/gzip/lz4/zstd compression
- Partitioning: composite primary key hashing to preserve ordering
- Recovery: LSN checkpoints enable resume from last processed position
- PII handling: SHA-256 hashing, partial masking, full redaction at field level

## Status

Active learning project. Built to understand CDC patterns, Kafka internals, and Go concurrency primitives. Not production-ready but functional for personal use cases.
