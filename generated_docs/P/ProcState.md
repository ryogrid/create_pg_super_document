# ProcState

## Location
src/backend/storage/ipc/sinvaladt.c: 138 - 163

## Overview
ProcState is a structure that represents per-backend state information in PostgreSQL's shared invalidation system, tracking message processing status and backend characteristics for cache invalidation coordination.

## Definition
```c
typedef struct ProcState
{
    pid_t           procPid;        /* PID of backend, for signaling */
    int             nextMsgNum;     /* next message number to read */
    bool            resetState;     /* backend needs to reset its state */
    bool            signaled;       /* backend has been sent catchup signal */
    bool            hasMessages;    /* backend has unread messages */
    bool            sendOnly;       /* backend only sends, never receives */
    LocalTransactionId nextLXID;    /* Next LocalTransactionId for idle backend */
} ProcState;
```

## Detailed Description
ProcState maintains the state of individual backend processes within PostgreSQL's shared invalidation system. This structure is essential for coordinating cache invalidation messages between different backend processes. Each backend has an associated ProcState entry that tracks its position in the invalidation message queue, its communication status, and special operating modes.

The structure supports different backend operating modes, including send-only backends (typically the Startup process during recovery) that fire invalidation messages without maintaining a local cache. It also manages the coordination of catchup signals and tracks whether backends have pending unread messages.

## Parameters / Member Variables
- `procPid`: Process ID of the backend, used for signaling; zero indicates an inactive entry
- `nextMsgNum`: Sequence number of the next invalidation message this backend should read
- `resetState`: Flag indicating the backend needs to reset its invalidation state
- `signaled`: Flag indicating the backend has been sent a catchup signal
- `hasMessages`: Flag indicating the backend has unread invalidation messages
- `sendOnly`: Flag for backends that only send invalidations but never receive them (e.g., Startup process)
- `nextLXID`: Next LocalTransactionId to use when this backend slot becomes idle

## Dependencies
- Functions called/Symbols referenced:
  - pid_t
  - LocalTransactionId
- Called from (representative examples):
  - SISeg (as array member)
  - SharedInvalBackendInit
  - CleanupInvalidationState
  - SIInsertDataEntries
  - SIGetDataEntries
  - SICleanupQueue

## Notes and Other Information
- The procPid field serves as both identification and activity indicator; zero values mark inactive entries
- The nextMsgNum field is only meaningful when procPid is non-zero and resetState is false
- The sendOnly mode is specifically designed for the Startup process during recovery, allowing it to send schema change notifications without maintaining a local relation cache
- The nextLXID field is maintained for convenience in copying values to/from local memory when MyProcNumber is set
- This structure is part of the larger shared invalidation infrastructure that ensures cache consistency across multiple PostgreSQL backend processes