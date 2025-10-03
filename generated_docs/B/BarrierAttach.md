# BarrierAttach

## Location
[src/backend/storage/ipc/barrier.c:236-255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/barrier.c#L236-L255)

## Overview
Attaches a new participant to a dynamic barrier, increasing the participant count and requiring the new participant to synchronize with other participants.

## Definition

```c
int
BarrierAttach(Barrier *barrier)
```
## Detailed Description
BarrierAttach adds a new participant to a dynamic barrier synchronization group. Once attached, the participant becomes part of the synchronization protocol and must participate in barrier operations before other participants can proceed.

The function performs the following operations atomically:
1. Increments the participant count in the barrier
2. Captures and returns the current phase number
3. Makes all existing waiting participants wait for this new participant

After attachment, the new participant must eventually call one of the barrier participation functions:
-  - to synchronize and wait
-  - to leave without arriving  
-  - to arrive and leave atomically

The returned phase number allows the new participant to understand which synchronization phase is currently active.

## Parameters / Member Variables
- : Pointer to the dynamic Barrier structure to attach to

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease
  - [Barrier](Barrier.md) (struct type)
- Called from (representative examples):
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)
  - [ExecHashTableCreate](../E/ExecHashTableCreate.md)
  - [ExecParallelHashJoinSetUpBatches](../E/ExecParallelHashJoinSetUpBatches.md)
  - [ExecParallelHashJoinNewBatch](../E/ExecParallelHashJoinNewBatch.md)

## Notes and Other Information
- Only valid for dynamic barriers (static_party must be false) - will assert if used on static barriers
- Returns the current phase number, which can be used to detect if phases have advanced since attachment
- Once attached, the participant becomes a blocking factor for other participants' synchronization
- The attachment is permanent until the participant explicitly detaches using detach functions
- Used primarily in parallel hash operations when new workers join the synchronization group
- Thread-safe operation protected by spinlock for concurrent access
- Critical for dynamic parallel processing where the number of participants can change during execution

## Simplified Source

```c
int
BarrierAttach(Barrier *barrier)
{
    int phase;

    // Only allow attachment to dynamic barriers
    Assert(!barrier->static_party);

    // Atomically increment participant count and get current phase
    SpinLockAcquire(&barrier->mutex);
    ++barrier->participants;
    phase = barrier->phase;
    SpinLockRelease(&barrier->mutex);

    return phase;
}
```

This simplified version shows the core barrier attachment: atomically increment the participant count and return the current phase for synchronization purposes.