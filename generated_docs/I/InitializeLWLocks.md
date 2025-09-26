# InitializeLWLocks

## Location
[src/backend/storage/lmgr/lwlock.c:493-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L493-L559)

## Overview
Initializes all fixed LWLocks and those belonging to named tranches by setting up their individual lock structures and organizing them into appropriate partitions.

## Definition

```c
static void
InitializeLWLocks(void)
```
## Detailed Description
InitializeLWLocks is a static function that performs the detailed initialization of all LWLock structures within the MainLWLockArray. It handles several categories of locks:

1. **Individual LWLocks**: Initializes all basic individual locks with unique IDs
2. **Buffer Mapping LWLocks**: Sets up partitioned locks for buffer management with LWTRANCHE_BUFFER_MAPPING tranche ID
3. **Lock Manager LWLocks**: Initializes partitioned locks for lock management with LWTRANCHE_LOCK_MANAGER tranche ID  
4. **Predicate Lock Manager LWLocks**: Sets up partitioned locks for serializable isolation with LWTRANCHE_PREDICATE_LOCK_MANAGER tranche ID
5. **Named Tranches**: Copies named tranche information into shared memory and initializes the requested LWLocks for extensions

The function ensures proper organization of locks by calculating offsets and systematically initializing each lock with its appropriate tranche ID for debugging and monitoring purposes.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - NumLWLocksForNamedTranches: Calculates number of locks needed for named tranches
  - LWLockInitialize: Initializes individual lock structures
  - LWLockNewTrancheId: Allocates new tranche IDs for named tranches
- Constants used:
  - NUM_INDIVIDUAL_LWLOCKS: Number of basic individual locks
  - BUFFER_MAPPING_LWLOCK_OFFSET: Offset for buffer mapping locks
  - NUM_BUFFER_PARTITIONS: Number of buffer partitions
  - LOCK_MANAGER_LWLOCK_OFFSET: Offset for lock manager locks
  - NUM_LOCK_PARTITIONS: Number of lock manager partitions
  - PREDICATELOCK_MANAGER_LWLOCK_OFFSET: Offset for predicate locks
  - NUM_PREDICATELOCK_PARTITIONS: Number of predicate lock partitions
  - NUM_FIXED_LWLOCKS: Total number of fixed locks
- Tranche IDs used:
  - LWTRANCHE_BUFFER_MAPPING: For buffer mapping locks
  - LWTRANCHE_LOCK_MANAGER: For lock manager locks
  - LWTRANCHE_PREDICATE_LOCK_MANAGER: For predicate lock locks
- Called from:
  - CreateLWLocks: Main LWLock creation function

## Notes and Other Information
- This is a static function, only called internally from CreateLWLocks
- Properly handles memory layout by calculating appropriate offsets for different lock types
- Named tranches support allows extensions to request dedicated LWLock groups
- Tranche names are copied into shared memory for visibility across processes
- The function maintains strict ordering: individual locks, then partitioned system locks, then named extension locks
- All locks are initialized through LWLockInitialize which sets up the internal lock state