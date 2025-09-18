# _bt_check_unique

## Location
src/backend/access/nbtree/nbtinsert.c: 408 - 814

## Overview
Checks for violations of unique index constraints by scanning for conflicting tuples and handling transaction wait scenarios.

## Definition


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
  - _bt_binsrch_insert: Performs binary search to find equal tuples
  - _bt_compare: Compares scan keys with page items
  - table_index_fetch_tuple_check: Checks heap tuple visibility
  - ItemPointerCompare: Compares tuple identifiers
  - BTreeTupleGetPostingN: Extracts TIDs from posting list tuples
  - CheckForSerializableConflictIn: Checks for serializable conflicts
  - BuildIndexValueDescription: Creates human-readable key description for error reporting
- Called from (representative examples):
  - _bt_doinsert: Main insertion routine that requires uniqueness validation

## Notes and Other Information
- Returns InvalidTransactionId when no conflict, otherwise returns transaction ID to wait for
- Sets bounds_valid state in insertstate for later reuse by _bt_findinsertloc
- Treats NULLs as equal, different from default unique index semantics (caller must handle NULL values appropriately)
- Implements fastpath optimization using cached binary search bounds to avoid redundant comparisons
- Supports CREATE INDEX CONCURRENTLY by checking if inserting tuple itself became dead
- Handles posting list tuples by processing each heap TID individually
- Marks dead tuples as killed and sets BTP_HAS_GARBAGE flag for cleanup
- For UNIQUE_CHECK_PARTIAL mode, never waits for other transactions