# GetNewMultiXactId

## Location
src/backend/access/transam/multixact.c: 1026 - 1178

## Overview
Assigns a new MultiXactId and reserves the required space in the members area, with comprehensive wraparound protection and autovacuum triggering mechanisms.

## Definition
```c
static MultiXactId GetNewMultiXactId(int nmembers, MultiXactOffset *offset)
```

## Detailed Description
GetNewMultiXactId is a critical static function that manages the allocation of new MultiXact IDs while ensuring system safety and preventing data loss from wraparound conditions. The function performs multiple layers of protection: it checks various limits (vacuum, warning, and stop limits) to prevent MultiXact ID wraparound, triggers autovacuum when necessary, and manages both MultiXact ID and member offset allocation.

The function implements sophisticated wraparound protection by monitoring several thresholds and taking appropriate action when limits are approached. It can issue warnings, trigger autovacuum processes, or refuse to allocate new IDs when safety limits are exceeded. The function also ensures proper space allocation in SLRU files and maintains critical sections to ensure atomicity of counter updates.

## Parameters / Member Variables
- `nmembers`: Number of transaction members that will be stored for this MultiXact
- `offset`: Pointer to MultiXactOffset variable that receives the starting offset in the members file where this MultiXact's members will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (recovery state checking)
  - LWLockAcquire, LWLockRelease (with MultiXactGenLock)
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md) (wraparound-aware comparison)
  - SendPostmasterSignal (autovacuum triggering)
  - [get_database_name](../g/get_database_name.md) (database name lookup)
  - ereport, errmsg_plural (error/warning reporting)
  - [ExtendMultiXactOffset](../E/ExtendMultiXactOffset.md), ExtendMultiXactMember (SLRU file extension)
  - [MultiXactOffsetWouldWrap](../M/MultiXactOffsetWouldWrap.md) (wraparound checking)
  - START_CRIT_SECTION (critical section management)
  - debug_elog3, debug_elog4 (debugging)
- Called from (representative examples):
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md) (during MultiXact creation)

## Notes and Other Information
- Function is marked static and used internally within the MultiXact subsystem
- Implements comprehensive wraparound protection with multiple threshold levels
- Automatically triggers autovacuum when approaching dangerous conditions
- Uses critical sections to ensure atomic updates of shared counters
- Handles recovery scenarios by preventing MultiXact assignment during recovery
- Manages both MultiXact ID assignment and member space reservation
- Implements safety checks to prevent catastrophic data loss
- Returns offset 1 instead of 0 to avoid issues with invalid offset values
- The caller must end the critical section after writing SLRU data