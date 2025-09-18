# WalRcvData

## Location
src/include/replication/walreceiver.h: 162 - 190

## Overview
WalRcvData is a shared memory structure that manages the state and configuration of the WAL receiver process in PostgreSQL physical replication.

## Definition
```c
typedef struct
{
    pid_t       pid;
    WalRcvState walRcvState;
    ConditionVariable walRcvStoppedCV;
    pg_time_t   startTime;
    XLogRecPtr  receiveStart;
    TimeLineID  receiveStartTLI;
    XLogRecPtr  flushedUpto;
    TimeLineID  receivedTLI;
    XLogRecPtr  latestChunkStart;
    TimestampTz lastMsgSendTime;
    TimestampTz lastMsgReceiptTime;
    XLogRecPtr  latestWalEnd;
    TimestampTz latestWalEndTime;
    char        conninfo[MAXCONNINFO];
    char        sender_host[NI_MAXHOST];
    int         sender_port;
    char        slotname[NAMEDATALEN];
    bool        is_temp_slot;
    bool        ready_to_display;
    Latch      *latch;
    slock_t     mutex;
    pg_atomic_uint64 writtenUpto;
    sig_atomic_t force_reply;
} WalRcvData;
```

## Detailed Description
WalRcvData serves as the central coordination structure for WAL receiver processes in PostgreSQL streaming replication. It resides in shared memory and is accessed by both the startup process and the walreceiver process to coordinate WAL streaming operations. The structure tracks the current state of replication, progress information, connection details, and synchronization primitives needed for safe concurrent access.

The structure is designed to handle both process lifecycle management (PID tracking, state transitions) and replication progress tracking (LSN positions, timelines, flush status). It uses atomic operations and spinlocks to ensure thread-safe access to shared data.

## Parameters / Member Variables
- `pid`: Process ID of the currently active walreceiver process
- `walRcvState`: Current state of the walreceiver (STOPPED, STARTING, STREAMING, WAITING, RESTARTING, STOPPING)
- `walRcvStoppedCV`: Condition variable for waiting on walreceiver state changes
- `startTime`: Time when the walreceiver was requested to be started
- `receiveStart`: LSN position where streaming should begin
- `receiveStartTLI`: Timeline ID for the starting position
- `flushedUpto`: Last byte position that has been received and flushed to disk
- `receivedTLI`: Timeline ID of the received WAL data
- `latestChunkStart`: Starting position of the current batch of received WAL
- `lastMsgSendTime`: Timestamp of the last message sent
- `lastMsgReceiptTime`: Timestamp of the last message received
- `latestWalEnd`: Latest reported end of WAL on the sender
- `latestWalEndTime`: Timestamp of the latest WAL end report
- `conninfo`: Connection string for connecting to the primary (security-sensitive fields obfuscated)
- `sender_host`: Host name, IP address, or directory path of the replication connection
- `sender_port`: Port number of the active replication connection
- `slotname`: Name of the replication slot used for connection
- `is_temp_slot`: Flag indicating if the replication slot is temporary and needs recreation
- `ready_to_display`: Flag indicating when conninfo is ready for display (passwords obfuscated)
- `latch`: Latch used by startup process to wake up walreceiver
- `mutex`: Spinlock protecting access to shared variables
- `writtenUpto`: Atomic counter tracking written position (advanced before flushing)
- `force_reply`: Atomic flag to force walreceiver reply (used as boolean)

## Dependencies
- Functions called/Symbols referenced:
  - WalRcvState (enum)
  - ConditionVariable
  - XLogRecPtr
  - TimeLineID
  - TimestampTz
  - Latch
  - slock_t
  - pg_atomic_uint64
  - sig_atomic_t

- Called from (representative examples):
  - WalReceiverMain
  - WalRcvWaitForStartPosition
  - WalRcvDie
  - XLogWalRcvFlush
  - ProcessWalSndrMessage
  - WalRcvShmemInit
  - WalRcvRunning
  - WalRcvStreaming
  - ShutdownWalRcv
  - RequestXLogStreaming
  - GetWalRcvFlushRecPtr

## Notes and Other Information
- Accessed through the global WalRcv pointer declared as `extern PGDLLIMPORT WalRcvData *WalRcv`
- The structure is allocated in shared memory and must be accessed with appropriate locking
- Some fields like `force_reply` use atomic semantics for lockless access with memory barriers
- The `writtenUpto` field is designed for lock-free reads while maintaining data consistency
- Connection information is obfuscated for security when displayed to users
- Used extensively in PostgreSQL streaming replication for coordinating between startup and walreceiver processes