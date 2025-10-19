# CreateBlockRefTableEntry

## Location
[src/common/blkreftable.c:875-893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L875-L893)

## Overview
CreateBlockRefTableEntry allocates and initializes a standalone BlockRefTableEntry for a specific relation fork, enabling incremental block reference table construction without requiring the entire table in memory.

## Definition
```c
BlockRefTableEntry *CreateBlockRefTableEntry(RelFileLocator rlocator, ForkNumber forknum)
```

## Detailed Description
This function creates a standalone BlockRefTableEntry that represents block modification tracking for a specific relation fork. Unlike entries in a full in-memory BlockRefTable (which are managed by simplehash), these standalone entries are independently allocated and managed, allowing for incremental construction of block reference tables without requiring the entire structure in memory.

The function initializes the entry with the provided relation file locator and fork number, and sets the limit block to InvalidBlockNumber as the default state. This entry can then be manipulated using BlockRefTableEntrySetLimitBlock and BlockRefTableEntryMarkBlockModified, written using BlockRefTableWriteEntry, and finally freed using BlockRefTableFreeEntry.

## Parameters / Member Variables
- `rlocator`: RelFileLocator identifying the specific relation file (database, tablespace, relfilenumber)
- `forknum`: ForkNumber specifying which fork of the relation (main, fsm, vm, init)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - memcpy (copies the RelFileLocator structure)
  - [BlockRefTableEntry](../B/BlockRefTableEntry.md) (return type structure)
  - [RelFileLocator](../R/RelFileLocator.md) (file location parameter type)
  - [ForkNumber](../F/ForkNumber.md) (fork number parameter type)
  - InvalidBlockNumber (constant for default limit block)

- Called from (representative examples):
  - Incremental block reference table construction code
  - Backup utilities that process relations one at a time
  - WAL processing that needs to track block modifications per relation fork

## Notes and Other Information
- This creates standalone entries, unlike hash table entries in full in-memory BlockRefTables
- The entry is zero-initialized, ensuring clean initial state for all fields
- limit_block is initialized to InvalidBlockNumber, indicating no limit is set initially
- The entry must be explicitly freed using BlockRefTableFreeEntry when no longer needed
- Designed for scenarios where memory efficiency is important and the full table doesn't need to be in memory
- Part of the incremental writing workflow: Create → Set/Mark → Write → Free
- The RelFileLocator is copied by value to ensure the entry owns its key data

## Simplified Source

```c
BlockRefTableEntry *
CreateBlockRefTableEntry(RelFileLocator rlocator, ForkNumber forknum)
{
    // Allocate and zero-initialize the entry structure
    BlockRefTableEntry *entry = palloc0(sizeof(BlockRefTableEntry));

    // Set the key fields for this relation fork
    memcpy(&entry->key.rlocator, &rlocator, sizeof(RelFileLocator));
    entry->key.forknum = forknum;

    // Initialize limit block to invalid (no limit set)
    entry->limit_block = InvalidBlockNumber;

    return entry;
}
```