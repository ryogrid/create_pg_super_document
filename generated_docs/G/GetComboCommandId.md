# GetComboCommandId

## Location
[src/backend/utils/time/combocid.c:204-278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/combocid.c#L204-L278)

## Overview
Creates or reuses a combo command ID that represents a combination of insertion (Cmin) and deletion/modification (Cmax) command IDs within a single transaction.

## Definition
```c
static CommandId GetComboCommandId(CommandId cmin, CommandId cmax)
```

## Detailed Description
This static function is the core of PostgreSQL's combo command ID mechanism. It manages the creation and reuse of combo command IDs, which are essential when a transaction needs to track more than the normal 62 command limit or when a tuple is both inserted and modified within the same transaction.

The function implements a hash table-based caching system to reuse existing combo CIDs with identical cmin/cmax pairs. On first use within a transaction, it initializes:
- A hash table (comboHash) for fast combo CID lookup
- An array (comboCids) to store the actual cmin/cmax mappings
- Size tracking variables for dynamic array growth

The function ensures the array has sufficient space before creating new entries to maintain data structure consistency. It uses TopTransactionContext for all allocations to ensure proper cleanup at transaction end.

## Parameters / Member Variables
- `cmin`: The command ID when the tuple was inserted
- `cmax`: The command ID when the tuple was deleted/modified

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAlloc: Allocates memory in TopTransactionContext
  - hash_create: Creates the combo CID hash table
  - hash_search: Looks up or creates hash table entries
  - repalloc: Expands the combo CID array when needed
- Called from (representative examples):
  - HeapTupleHeaderAdjustCmax: When determining combo CID for tuple deletion
  - RestoreComboCIDState: During transaction state restoration

## Notes and Other Information
- Static function - internal to combocid.c module
- Implements efficient reuse of combo CIDs to save space
- Hash table uses HASH_ELEM | HASH_BLOBS | HASH_CONTEXT for configuration
- Array grows by doubling size when full (starts at CCID_ARRAY_SIZE)
- Critical for MVCC when tuples are inserted and modified in same transaction
- Part of mechanism allowing transactions to exceed 62-command limit
- Uses TopTransactionContext to ensure transaction-scoped lifetime
- Located in src/backend/utils/time/combocid.c:204-278