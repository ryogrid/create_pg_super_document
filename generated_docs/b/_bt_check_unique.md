# _bt_check_unique

## Location
[src/backend/access/nbtree/nbtinsert.c:408-814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L408-L814)

## Overview
Checks for violations of unique index constraints by scanning for conflicting tuples and handling transaction wait scenarios.

## Definition

```c
static TransactionId
_bt_check_unique(Relation rel, BTInsertState insertstate, Relation heapRel,
				 IndexUniqueCheck checkUnique, bool *is_unique,
				 uint32 *speculativeToken)
```
## Detailed Description
The  function performs uniqueness constraint validation for B-tree index insertions. It scans through tuples with the same key as the tuple being inserted to detect conflicts. The function handles various scenarios including live conflicts, transactions in progress, and dead tuples.

The function supports different checking modes: full checking with error reporting (UNIQUE_CHECK_YES), existence checking without insertion (UNIQUE_CHECK_EXISTING), and partial checking that returns immediately on potential conflicts (UNIQUE_CHECK_PARTIAL). It implements sophisticated handling of concurrent transactions, including waiting for speculative insertions and other pending transactions.

A key optimization is the ability to mark dead tuples as killed when all HOT chain members are confirmed dead, helping with index cleanup. The function also handles posting list tuples by iterating through all heap TIDs within them.

## Parameters / Member Variables
- : The B-tree index relation being checked
- : Current insertion state containing the tuple and search context
- : The heap relation associated with the index
- : Type of uniqueness check to perform (NO/PARTIAL/YES/EXISTING)
- : Output parameter set to false if potential conflict found
- : Output parameter for speculative insertion token when waiting needed

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_binsrch_insert](_bt_binsrch_insert.md): Performs binary search to find equal tuples
  - [_bt_compare](_bt_compare.md): Compares scan keys with page items
  - [table_index_fetch_tuple_check](../t/table_index_fetch_tuple_check.md): Checks heap tuple visibility
  - [ItemPointerCompare](../I/ItemPointerCompare.md): Compares tuple identifiers
  - [BTreeTupleGetPostingN](../B/BTreeTupleGetPostingN.md): Extracts TIDs from posting list tuples
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md): Checks for serializable conflicts
  - [BuildIndexValueDescription](../B/BuildIndexValueDescription.md): Creates human-readable key description for error reporting
- Called from (representative examples):
  - [_bt_doinsert](_bt_doinsert.md): Main insertion routine that requires uniqueness validation

## Notes and Other Information
- Returns InvalidTransactionId when no conflict, otherwise returns transaction ID to wait for
- Sets bounds_valid state in insertstate for later reuse by _bt_findinsertloc
- Treats NULLs as equal, different from default unique index semantics (caller must handle NULL values appropriately)
- Implements fastpath optimization using cached binary search bounds to avoid redundant comparisons
- Supports CREATE INDEX CONCURRENTLY by checking if inserting tuple itself became dead
- Handles posting list tuples by processing each heap TID individually
- Marks dead tuples as killed and sets BTP_HAS_GARBAGE flag for cleanup
- For UNIQUE_CHECK_PARTIAL mode, never waits for other transactions