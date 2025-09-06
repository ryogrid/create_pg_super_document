# PostgreSQL Process Entry Points

PostgreSQL consists of multiple processes, each with a specific role, and each process has its own entry point function.

## Overview

In PostgreSQL's architecture, the following processes work together:

- **postmaster**: Parent process (process management)
- **backend process**: Handles client connections
- **walwriter**: Writes to WAL files
- **walsender**: Sends WAL data for replication
- **walreceiver**: Receives WAL data for replication
- **checkpointer**: Handles checkpoint processing
- **background writer**: Writes pages from the buffer pool to disk
- **startup process**: Handles recovery during database startup
- **logger**: Handles log output

## Entry Point Functions

### 1. postmaster (Parent Process)

- **Function Name**: `PostmasterMain`
- **File Path**: `src/backend/postmaster/postmaster.c`
- **Line Number**: 489
- **Role**: Acts as the parent process of PostgreSQL, managing child processes and accepting client connections.

### 2. backend process

- **Function Name**: `BackendMain`
- **File Path**: `src/backend/tcop/backend_startup.c`
- **Line Number**: 57
- **Role**: Handles client connections and executes SQL queries.

### 3. walwriter

- **Function Name**: `WalWriterMain`
- **File Path**: `src/backend/postmaster/walwriter.c`
- **Line Number**: 89
- **Role**: Asynchronously writes from the WAL buffer to disk.

### 4. walsender

- **Function Name**: `BackendMain` → `PostgresMain`
- **File Path**: 
  - `src/backend/tcop/backend_startup.c` (Line Number: 57)
  - `src/backend/tcop/postgres.c` (Line Number: 4239)
- **Role**: Sends WAL data to standby servers for replication.
- **Note**: Operates as a regular backend process and processes replication commands in `PostgresMain`.

### 5. walreceiver

- **Function Name**: `WalReceiverMain`
- **File Path**: `src/backend/replication/walreceiver.c`
- **Line Number**: 183
- **Role**: Receives WAL data from the primary server and applies it locally.

### 6. checkpointer

- **Function Name**: `CheckpointerMain`
- **File Path**: `src/backend/postmaster/checkpointer.c`
- **Line Number**: 176
- **Role**: Performs periodic checkpoint processing to ensure data durability.

### 7. background writer

- **Function Name**: `BackgroundWriterMain`
- **File Path**: `src/backend/postmaster/bgwriter.c`
- **Line Number**: 87
- **Role**: Efficiently writes pages from the buffer pool to disk.

### 8. startup process

- **Function Name**: `StartupProcessMain`
- **File Path**: `src/backend/postmaster/startup.c`
- **Line Number**: 216
- **Role**: Handles crash recovery and WAL replay during database startup.

### 9. logger

- **Function Name**: `SysLoggerMain`
- **File Path**: `src/backend/postmaster/syslogger.c`
- **Line Number**: 167
- **Role**: Collects and outputs system logs.

## Common Entry Points

### Main Entry Point

- **Function Name**: `main`
- **File Path**: `src/backend/main/main.c`
- **Line Number**: 59
- **Role**: The initial entry point for all PostgreSQL processes, dispatching to the appropriate Main function based on the process type.

### Single-User Mode

- **Function Name**: `PostgresSingleUserMain`
- **File Path**: `src/backend/tcop/postgres.c`
- **Line Number**: 4129
- **Role**: Provides database operations in single-user mode for maintenance purposes.

## Process Startup Flow

1. The `main()` function is executed first.
2. Based on command-line arguments, the appropriate Main function is selected.
3. Each process initializes and begins its main operations in its dedicated Main function.
4. The postmaster process starts and manages other processes as needed.

## Notes

- The WALSender process is unique in that it operates as a regular backend process.
- Inter-process coordination is achieved through shared memory and signals.
- Each process has its own memory space but manages shared resources appropriately.

---

*This document is based on the PostgreSQL 17.6 source code.*
