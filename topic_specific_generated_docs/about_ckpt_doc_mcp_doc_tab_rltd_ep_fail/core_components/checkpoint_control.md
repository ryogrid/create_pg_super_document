# Checkpoint Control Subsystem

*[← Back to Core Components](README.md)*

## Overview

The checkpoint control subsystem manages the orchestration and scheduling of checkpoints in PostgreSQL. It coordinates between the checkpointer background process, backend processes requesting checkpoints, and the actual checkpoint execution logic. This subsystem ensures database consistency by forcing all dirty buffers to disk at regular intervals or upon specific triggers.

## Key Concepts

- **Checkpoint Process**: Dedicated background process ([`CheckpointerMain`](#checkpointermain)) that handles checkpoint scheduling and execution
- **Request Interface**: Mechanism ([`RequestCheckpoint`](#requestcheckpoint)) for backend processes to trigger checkpoints
- **Checkpoint Flags**: Control parameters that determine checkpoint behavior and urgency
- **Shared Memory Coordination**: Communication between processes via [`CheckpointerShmem`](#checkpointershmem-structure) structure
- **Signal Handling**: Interrupt-driven checkpoint triggering via SIGINT

## Architecture

```mermaid
graph TB
    subgraph "Process Communication"
        BE[Backend Processes] -->|RequestCheckpoint| CS[CheckpointerShmem]
        CS -->|flags/signals| CP[CheckpointerMain]
        SIGINT[SIGINT Signal] --> CPH[ReqCheckpointHandler]
        CPH --> CS
    end

    subgraph "Checkpoint Control Loop"
        CP --> CL[Main Control Loop]
        CL -->|check flags| CD{Checkpoint Decision}
        CD -->|yes| CE[Checkpoint Execution]
        CD -->|no| SL[Sleep/Wait]
        SL --> CL
        CE --> CC[Checkpoint Completion]
        CC --> CL
    end

    subgraph "Execution Path"
        CE -->|recovery mode| CRP[CreateRestartPoint]
        CE -->|normal mode| CCP[CreateCheckPoint]
        CCP --> CPG[CheckPointGuts]
        CRP --> CPG
    end

    subgraph "Triggers"
        TT[Time Trigger<br/>checkpoint_timeout]
        WT[WAL Trigger<br/>max_wal_size]
        MT[Manual Trigger<br/>CHECKPOINT command]
        ST[Shutdown Trigger]
    end

    TT --> CD
    WT --> CD
    MT --> BE
    ST --> BE

    classDef processNode fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef controlNode fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef triggerNode fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px

    class BE,CP,CPH processNode
    class CS,CL,CD,CE,CC,CCP,CRP,CPG controlNode
    class TT,WT,MT,ST triggerNode
```

## Core APIs

### CheckpointerMain

#### Purpose
Main entry point for the checkpointer background process. Manages the complete lifecycle of checkpoint scheduling, coordination, and execution.

#### Signature
```c
void CheckpointerMain(char *startup_data, size_t startup_data_len);
```

#### Detailed Description
CheckpointerMain operates as an infinite loop that:

1. **Initialization Phase**:
   - Sets up process type as `B_CHECKPOINTER`
   - Configures signal handlers for checkpoint requests and shutdown
   - Initializes memory context and error handling
   - Sets up shared memory coordination structures

2. **Main Control Loop**:
   - Absorbs sync requests to prevent queue overflow
   - Handles interrupts and signals
   - Evaluates checkpoint triggers (time-based, WAL-based, manual)
   - Executes checkpoints or restart points as appropriate
   - Reports statistics and manages archive timeouts

3. **Error Recovery**:
   - Implements robust error handling with setjmp/longjmp
   - Cleans up resources on checkpoint failure
   - Notifies waiting backends of checkpoint status

#### Key Internal Logic Flow

1. **Trigger Evaluation**:
   ```c
   // Check for pending checkpoint request
   if (CheckpointerShmem->ckpt_flags) {
       do_checkpoint = true;
       chkpt_or_rstpt_requested = true;
   }

   // Time-based checkpoint trigger
   elapsed_secs = now - last_checkpoint_time;
   if (elapsed_secs >= CheckPointTimeout) {
       do_checkpoint = true;
       flags |= CHECKPOINT_CAUSE_TIME;
   }
   ```

2. **Checkpoint Execution Decision**:
   ```c
   // Determine checkpoint vs restart point
   do_restartpoint = RecoveryInProgress();
   if (flags & CHECKPOINT_END_OF_RECOVERY)
       do_restartpoint = false;

   // Execute appropriate checkpoint type
   if (!do_restartpoint)
       CreateCheckPoint(flags);
   else
       ckpt_performed = CreateRestartPoint(flags);
   ```

3. **Completion Notification**:
   ```c
   // Update shared memory state
   SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
   CheckpointerShmem->ckpt_done = CheckpointerShmem->ckpt_started;
   SpinLockRelease(&CheckpointerShmem->ckpt_lck);

   // Wake up waiting backends
   ConditionVariableBroadcast(&CheckpointerShmem->done_cv);
   ```

#### Integration Points
- **Called by**: [`AuxiliaryProcessMain`](../appendices/symbol_index.md#auxiliaryprocessmain) during postmaster startup
- **Calls**: [`CreateCheckPoint`](checkpoint_execution.md#createcheckpoint), [`CreateRestartPoint`](checkpoint_execution.md#createrestartpoint), [`AbsorbSyncRequests`](wal_coordination.md#absorbsyncrequests)
- **Shared state**: [`CheckpointerShmem`](#checkpointershmem-structure) structure for inter-process communication

#### Performance Characteristics
- Sleeps between checkpoints using WaitLatch for efficient CPU usage
- Balances checkpoint frequency with system load
- Adaptive timing based on WAL generation rate and configuration

### RequestCheckpoint

#### Purpose
Interface for backend processes to request checkpoints. Provides synchronous and asynchronous checkpoint initiation with various urgency levels.

#### Signature
```c
void RequestCheckpoint(int flags);
```

#### Parameters
| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| flags | int | Bitwise OR of checkpoint control flags | See flag definitions below |

#### Checkpoint Flags
- **CHECKPOINT_IS_SHUTDOWN**: Database shutdown checkpoint
- **CHECKPOINT_END_OF_RECOVERY**: End-of-recovery checkpoint
- **CHECKPOINT_IMMEDIATE**: Skip completion target, finish ASAP
- **CHECKPOINT_FORCE**: Force checkpoint even without WAL activity
- **CHECKPOINT_WAIT**: Wait for completion before returning
- **CHECKPOINT_CAUSE_XLOG**: Triggered by WAL volume

#### Internal Logic

1. **Standalone Backend Path**:
   ```c
   if (!IsPostmasterEnvironment) {
       CreateCheckPoint(flags | CHECKPOINT_IMMEDIATE);
       smgrdestroyall();
       return;
   }
   ```

2. **Shared Memory Request**:
   ```c
   SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
   old_failed = CheckpointerShmem->ckpt_failed;
   old_started = CheckpointerShmem->ckpt_started;
   CheckpointerShmem->ckpt_flags |= (flags | CHECKPOINT_REQUESTED);
   SpinLockRelease(&CheckpointerShmem->ckpt_lck);
   ```

3. **Checkpointer Notification**:
   ```c
   SetLatch(&ProcGlobal->checkpointerLatch);
   ```

4. **Wait Logic** (if CHECKPOINT_WAIT specified):
   - Monitors ckpt_started counter to confirm request acknowledgment
   - Waits on condition variable for checkpoint completion
   - Handles timeout and error scenarios

#### Integration Points
- **Called by**: Backend processes, CHECKPOINT SQL command, shutdown procedures
- **Calls**: [`CreateCheckPoint`](checkpoint_execution.md#createcheckpoint) (standalone mode), `SetLatch`
- **Shared state**: [`CheckpointerShmem`](#checkpointershmem-structure) for request queuing and status tracking

## Data Structures

### CheckpointerShmem Structure

```c
typedef struct CheckpointerShmemStruct {
    pid_t           checkpointer_pid;      // Checkpointer process ID

    slock_t         ckpt_lck;             // Spinlock for atomic updates

    int             ckpt_flags;           // Pending checkpoint request flags
    int             ckpt_started;         // Checkpoint start sequence number
    int             ckpt_done;            // Checkpoint completion sequence number
    int             ckpt_failed;          // Checkpoint failure sequence number

    ConditionVariable start_cv;          // Start coordination CV
    ConditionVariable done_cv;           // Completion notification CV

    uint32          num_backend_writes;   // Backend-triggered buffer writes
    uint32          num_backend_fsync;    // Backend-triggered fsync operations
} CheckpointerShmemStruct;
```

**Key Features**:
- **Atomic Counters**: Sequence numbers track checkpoint lifecycle
- **Condition Variables**: Efficient inter-process synchronization
- **Spinlock Protection**: Ensures atomic flag updates
- **Statistics Tracking**: Monitors backend-triggered I/O operations

## Processing Flow

```mermaid
sequenceDiagram
    participant BE as Backend Process
    participant CS as CheckpointerShmem
    participant CP as CheckpointerMain
    participant CCF as CreateCheckPoint
    participant CPG as CheckPointGuts
    participant BM as Buffer Manager

    Note over CP: Main Control Loop
    CP->>CP: ResetLatch()
    CP->>CP: AbsorbSyncRequests()
    CP->>CP: HandleCheckpointerInterrupts()

    alt Manual Checkpoint Request
        BE->>CS: RequestCheckpoint(flags)
        CS->>CP: SetLatch()
        Note over CS: ckpt_flags set
    else Time-based Trigger
        CP->>CP: Check elapsed_secs >= CheckPointTimeout
        Note over CP: Set CHECKPOINT_CAUSE_TIME
    else WAL-based Trigger
        CP->>CP: Check WAL volume/segments
        Note over CP: Set CHECKPOINT_CAUSE_XLOG
    end

    CP->>CS: Check ckpt_flags
    CS-->>CP: flags detected

    CP->>CP: Determine checkpoint vs restart point
    alt Normal Checkpoint
        CP->>CCF: CreateCheckPoint(flags)
        CCF->>CCF: Prepare checkpoint record
        CCF->>CPG: CheckPointGuts(redo, flags)
        CPG->>BM: CheckPointBuffers(flags)
        BM-->>CPG: Buffers flushed
        CPG->>CPG: ProcessSyncRequests()
        CPG-->>CCF: Checkpoint guts complete
        CCF->>CCF: Insert checkpoint WAL record
        CCF-->>CP: Checkpoint complete
    else Restart Point
        CP->>CP: CreateRestartPoint(flags)
        Note over CP: Similar to checkpoint but for recovery
    end

    CP->>CS: Update ckpt_done counter
    CS->>BE: ConditionVariableBroadcast()

    CP->>CP: CheckArchiveTimeout()
    CP->>CP: Report statistics
    CP->>CP: WaitLatch() until next trigger
```

## Implementation Notes

### Signal Handling Strategy
The checkpointer process uses a sophisticated signal handling approach:
- **SIGINT**: Triggers checkpoint requests via [`ReqCheckpointHandler`](#reqcheckpointhandler)
- **SIGUSR2**: Handles shutdown requests
- **SIGTERM**: Deliberately ignored to allow graceful shutdown coordination

### Memory Management
- Dedicated memory context (`checkpointer_context`) for checkpoint operations
- Context reset on error recovery to prevent memory leaks
- Strategic memory allocation patterns for large data structures

### Concurrency Control
- Spinlocks protect critical shared memory updates
- Condition variables coordinate process synchronization
- Lock-free atomic operations for performance-critical paths

### Error Recovery
- Setjmp/longjmp exception handling at the process level
- Comprehensive resource cleanup on checkpoint failure
- Graceful degradation and retry mechanisms

## Configuration Integration

### Key Parameters
- **checkpoint_timeout**: Time-based checkpoint interval (default: 300s)
- **max_wal_size**: WAL volume checkpoint trigger (default: 1GB)
- **checkpoint_completion_target**: I/O spreading target (default: 0.9)
- **checkpoint_warning**: Log threshold for frequent checkpoints (default: 30s)

### Adaptive Behavior
- Dynamic adjustment of checkpoint timing based on system load
- WAL generation rate influences checkpoint scheduling
- Background writer coordination reduces checkpoint I/O spikes

## Performance Characteristics

### Scalability Factors
- Linear scaling with buffer pool size
- Tablespace parallelization for I/O distribution
- Checkpoint spreading algorithms minimize performance impact

### Bottleneck Identification
- I/O subsystem typically limits checkpoint performance
- Lock contention during critical sections
- WAL flush performance affects checkpoint completion time

### Optimization Strategies
- Background writer pre-cleaning reduces checkpoint work
- Intelligent buffer sorting minimizes random I/O
- Adaptive throttling balances performance and consistency requirements

## Related Components

- **[Checkpoint Execution](checkpoint_execution.md)**: Core checkpoint implementation
- **[Buffer Management](buffer_management.md)**: Dirty buffer synchronization
- **[WAL Coordination](wal_coordination.md)**: Write-ahead log integration
- **[Performance Tuning](../performance_tuning.md)**: Configuration optimization

## Cross-References

### Functions Called
- [`CreateCheckPoint`](checkpoint_execution.md#createcheckpoint)
- [`CreateRestartPoint`](checkpoint_execution.md#createrestartpoint)
- [`AbsorbSyncRequests`](wal_coordination.md#absorbsyncrequests)
- [`CheckArchiveTimeout`](wal_coordination.md#checkarchivetimeout)

### Functions Calling
- [`AuxiliaryProcessMain`](../appendices/symbol_index.md#auxiliaryprocessmain)

### Data Structures Used
- [`CheckpointerShmemStruct`](#checkpointershmem-structure)
- [`CheckpointStatsData`](../appendices/symbol_index.md#checkpointstatsdata)

*[← Back to Core Components](README.md) | [Next: Checkpoint Execution →](checkpoint_execution.md)*