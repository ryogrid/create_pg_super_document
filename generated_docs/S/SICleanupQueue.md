# SICleanupQueue

## Location
[src/backend/storage/ipc/sinvaladt.c:577-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinvaladt.c#L577-L699)

## Overview
Removes consumed shared invalidation messages from the queue and manages backend synchronization by signaling lagging backends and forcing reset states when necessary.

## Definition
```c
void SICleanupQueue(bool callerHasWriteLock, int minFree)
```

## Detailed Description
SICleanupQueue is a critical maintenance function in PostgreSQL's shared invalidation system that performs garbage collection on the message queue and ensures all backends stay reasonably synchronized. The function operates by computing the minimum message number across all active backends and removing messages that have been consumed by everyone.

The function implements sophisticated backend management logic, including identifying backends that have fallen too far behind and either signaling them to catch up or forcing them into reset state. It uses a two-phase approach: first acquiring exclusive locks to examine and update the queue state, then potentially releasing locks to send signals to lagging backends.

The implementation includes overflow protection by periodically decrementing all message counters when they grow too large, and dynamically adjusts cleanup thresholds based on current queue utilization to optimize performance.

## Parameters / Member Variables
- `callerHasWriteLock`: Boolean indicating whether the caller already holds SInvalWriteLock (avoids redundant lock acquisition)
- `minFree`: Minimum number of message slots that must be made available in the queue

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for SInvalWriteLock and SInvalReadLock)
  - [SendProcSignal](SendProcSignal.md) (to send PROCSIG_CATCHUP_INTERRUPT)
  - elog (for debug logging)
  - [SISeg](SISeg.md) (shared invalidation segment structure)
  - [ProcState](../P/ProcState.md) (per-process state tracking)
  - SIG_THRESHOLD, MAXNUMMESSAGES, MSGNUMWRAPAROUND (configuration constants)
  - CLEANUP_MIN, CLEANUP_QUANTUM (cleanup threshold constants)
- Called from (representative examples):
  - [SIInsertDataEntries](SIInsertDataEntries.md) (when queue space is needed)
  - Periodic maintenance routines in sinval.c

## Notes and Other Information
- Uses exclusive locking on both SInvalWriteLock and SInvalReadLock for atomic queue modification
- Implements overflow protection through counter wraparound when MSGNUMWRAPAROUND threshold is reached
- Signals at most one backend per call to avoid thundering herd problems
- Ignores sendOnly backends in minimum calculations to prevent blocking message insertion
- Forces backends into reset state if they fall more than MAXNUMMESSAGES-minFree behind
- Dynamically adjusts nextThreshold based on current queue utilization for optimal cleanup frequency
- May temporarily release and reacquire locks when signaling backends, so minFree guarantee is not absolute
- Part of PostgreSQL's distributed cache invalidation system ensuring bounded memory usage and backend synchronization

## Simplified Source

```c
void SICleanupQueue(bool callerHasWriteLock, int minFree) {
    SISeg *segP = shmInvalBuffer;
    int min, minsig, lowbound, numMsgs;
    ProcState *needSig = NULL;

    // Acquire exclusive locks for queue modification
    if (!callerHasWriteLock)
        LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);
    LWLockAcquire(SInvalReadLock, LW_EXCLUSIVE);

    // Find minimum message number across all backends
    min = segP->maxMsgNum;
    minsig = min - SIG_THRESHOLD;
    lowbound = min - MAXNUMMESSAGES + minFree;

    // Check each active backend
    for (int i = 0; i < segP->numProcs; i++) {
        ProcState *stateP = &segP->procState[segP->pgprocnos[i]];
        int n = stateP->nextMsgNum;

        // Skip reset or send-only backends
        if (stateP->resetState || stateP->sendOnly)
            continue;

        // Force reset if backend is too far behind
        if (n < lowbound) {
            stateP->resetState = true;
            continue;
        }

        // Track global minimum and find backend needing signal
        if (n < min)
            min = n;
        if (n < minsig && !stateP->signaled) {
            minsig = n;
            needSig = stateP;
        }
    }
    segP->minMsgNum = min;

    // Handle counter overflow by wrapping around
    if (min >= MSGNUMWRAPAROUND) {
        segP->minMsgNum -= MSGNUMWRAPAROUND;
        segP->maxMsgNum -= MSGNUMWRAPAROUND;
        for (int i = 0; i < segP->numProcs; i++)
            segP->procState[segP->pgprocnos[i]].nextMsgNum -= MSGNUMWRAPAROUND;
    }

    // Set next cleanup threshold
    numMsgs = segP->maxMsgNum - segP->minMsgNum;
    segP->nextThreshold = (numMsgs < CLEANUP_MIN) ?
        CLEANUP_MIN : (numMsgs / CLEANUP_QUANTUM + 1) * CLEANUP_QUANTUM;

    // Signal lagging backend if needed
    if (needSig) {
        pid_t his_pid = needSig->procPid;
        ProcNumber his_procNumber = (needSig - &segP->procState[0]);

        needSig->signaled = true;
        LWLockRelease(SInvalReadLock);
        LWLockRelease(SInvalWriteLock);

        SendProcSignal(his_pid, PROCSIG_CATCHUP_INTERRUPT, his_procNumber);

        if (callerHasWriteLock)
            LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);
    } else {
        // Clean release of locks
        LWLockRelease(SInvalReadLock);
        if (!callerHasWriteLock)
            LWLockRelease(SInvalWriteLock);
    }
}
```