# PredicateLockShmemSize

## Location
[src/backend/storage/lmgr/predicate.c:1347-1408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1347-L1408)

## Overview
PredicateLockShmemSize calculates the total shared memory space required for all predicate locking data structures used in PostgreSQL's serializable snapshot isolation implementation.

## Definition
```c
Size PredicateLockShmemSize(void)
```

## Detailed Description
This function estimates the shared memory requirements for the entire predicate locking system by calculating the space needed for each component and summing them together. The calculations must exactly match the allocations performed in InitPredicateLocks() to ensure sufficient memory is available during startup.

The function calculates memory for:

1. **Predicate Lock Target Hash Table**: Uses NPREDICATELOCKTARGETENTS() to determine the number of PREDICATELOCKTARGET entries
2. **Predicate Lock Hash Table**: Assumes 2x the target entries for PREDICATELOCK structs (2 transactions per target)
3. **Safety Margin**: Adds 10% extra space since NPREDICATELOCKTARGETENTS is only an estimate
4. **Transaction List**: Space for SERIALIZABLEXACT structures, assuming 10x (MaxBackends + max_prepared_xacts)
5. **Transaction XID Table**: Hash table for SERIALIZABLEXID entries
6. **RW-Conflict Pool**: Conflict tracking structures, assuming 5x the transaction count
7. **Finished Transaction List**: Header for completed serializable transactions
8. **Serial SLRU Components**: SerialControlData structure and SLRU buffer space

The function uses PostgreSQL's size calculation utilities (add_size, mul_size) to safely handle potential overflow conditions.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - NPREDICATELOCKTARGETENTS
  - [hash_estimate_size](../h/hash_estimate_size.md)
  - [add_size](../a/add_size.md)
  - [mul_size](../m/mul_size.md)
  - [SimpleLruShmemSize](../S/SimpleLruShmemSize.md)
  - Various size constants (PredXactListDataSize, RWConflictPoolHeaderDataSize, etc.)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md)

## Notes and Other Information
- This is a public function accessible outside predicate.c
- Critical for PostgreSQL startup memory planning - must be called before shared memory allocation
- Size calculations must exactly match the allocation logic in InitPredicateLocks()
- Includes a 10% safety margin to account for estimation uncertainties in NPREDICATELOCKTARGETENTS
- The sizing assumptions mirror those in InitPredicateLocks(): 2 xacts per target, 10 predicate locking transactions per backend, 5 conflicts per transaction
- Uses PostgreSQL's overflow-safe arithmetic functions to prevent integer overflow
- Part of the shared memory size calculation infrastructure used during database cluster startup

## Simplified Source

```c
// Simplified version of PredicateLockShmemSize
Size PredicateLockShmemSize(void) {
    Size total_size = 0;
    long max_entries;

    // Calculate space for predicate lock target hash table
    max_entries = NPREDICATELOCKTARGETENTS();
    total_size = add_size(total_size,
        hash_estimate_size(max_entries, sizeof(PREDICATELOCKTARGET)));

    // Calculate space for predicate lock hash table (2x targets)
    max_entries *= 2;
    total_size = add_size(total_size,
        hash_estimate_size(max_entries, sizeof(PREDICATELOCK)));

    // Add 10% safety margin for estimation uncertainty
    total_size = add_size(total_size, total_size / 10);

    // Calculate space for serializable transaction structures
    max_entries = (MaxBackends + max_prepared_xacts) * 10;
    total_size = add_size(total_size, PredXactListDataSize);
    total_size = add_size(total_size,
        mul_size((Size) max_entries, sizeof(SERIALIZABLEXACT)));

    // Calculate space for transaction XID hash table
    total_size = add_size(total_size,
        hash_estimate_size(max_entries, sizeof(SERIALIZABLEXID)));

    // Calculate space for read-write conflict pool (5x transactions)
    max_entries *= 5;
    total_size = add_size(total_size, RWConflictPoolHeaderDataSize);
    total_size = add_size(total_size,
        mul_size((Size) max_entries, RWConflictDataSize));

    // Add space for finished transaction list and SLRU components
    total_size = add_size(total_size, sizeof(dlist_head));
    total_size = add_size(total_size, sizeof(SerialControlData));
    total_size = add_size(total_size, SimpleLruShmemSize(serializable_buffers, 0));

    return total_size;
}
```

Key simplifications made:
- Renamed variables for clarity (size → total_size, max_table_size → max_entries)
- Consolidated comments to explain each major calculation step
- Grouped related calculations together logically
- Maintained the exact same calculation logic and order
- Preserved all safety mechanisms and overflow protection