# LogicalRepWorkerType

## Location
src/include/replication/worker_internal.h: 35 - 36

## Overview
LogicalRepWorkerType is an enumeration that defines the different types of workers used in PostgreSQL logical replication system.

## Definition
```c
typedef enum LogicalRepWorkerType
{
    WORKERTYPE_UNKNOWN = 0,
    WORKERTYPE_TABLESYNC,
    WORKERTYPE_APPLY,
    WORKERTYPE_PARALLEL_APPLY,
} LogicalRepWorkerType;
```

## Detailed Description
This enum categorizes the various types of background workers that participate in PostgreSQL logical replication. Each worker type has a specific role in the replication process:

- **WORKERTYPE_UNKNOWN**: Initial/default state indicating an uninitialized or unknown worker type
- **WORKERTYPE_TABLESYNC**: Workers responsible for initial table synchronization when setting up logical replication
- **WORKERTYPE_APPLY**: Main apply workers that process incoming logical replication changes 
- **WORKERTYPE_PARALLEL_APPLY**: Parallel apply workers that can process changes concurrently for better performance

The enum is used throughout the logical replication subsystem to determine worker behavior, resource allocation, and operational constraints.

## Parameters / Member Variables
- `WORKERTYPE_UNKNOWN`: Default value (0) representing uninitialized worker state
- `WORKERTYPE_TABLESYNC`: Workers that perform initial table data synchronization
- `WORKERTYPE_APPLY`: Standard apply workers for processing replication stream
- `WORKERTYPE_PARALLEL_APPLY`: Parallel workers for concurrent change application

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Called from (representative examples):
  - `logicalrep_worker_launch` (src/backend/replication/logical/launcher.c:313)
  - `LogicalRepWorker` struct member (src/include/replication/worker_internal.h:40)

## Notes and Other Information
- The enum is defined in `src/include/replication/worker_internal.h:29-35`
- Used as the `type` field in the `LogicalRepWorker` struct to identify worker roles
- Critical for worker lifecycle management and proper resource allocation
- Tablesync workers are the only type that can have an associated relation ID (relid)
- Parallel apply workers are considered subworkers and have specific DSM (Dynamic Shared Memory) handling
- Worker type validation is enforced in `logicalrep_worker_launch` function