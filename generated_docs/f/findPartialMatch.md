# findPartialMatch

## Location
[src/backend/executor/nodeSubplan.c:744-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L744-L778)

## Overview
Searches a hash table to determine if it contains an entry that is not provably distinct from a given tuple, used in subplan execution for partial match detection.

## Definition

```c
static bool
findPartialMatch(TupleHashTable hashtable, TupleTableSlot *slot,
				 FmgrInfo *eqfunctions)
```
## Detailed Description
findPartialMatch performs a comprehensive scan of an entire hash table to detect whether any stored tuple could potentially match the given input tuple. Unlike typical hash table lookups that use hash keys for efficient probing, this function must examine every entry because partial matches may occur with tuples that have completely different hash values from the input tuple.

The function is specifically designed for subplan execution scenarios where cross-type comparisons are involved, requiring custom equality functions rather than the hash table's internal comparison functions. It uses the execTuplesUnequal function to perform the actual tuple comparison, implementing SQL's NULL semantics for proper partial match detection.

## Parameters / Member Variables
- `hashtable`: The TupleHashTable to search through for potential matches
- `*slot`: The TupleTableSlot containing the tuple to find partial matches for
- `*eqfunctions`: Array of FmgrInfo structures containing the equality functions to use for comparison (required for cross-type cases)
## Dependencies
- Functions called/Symbols referenced:
  - InitTupleHashIterator
  - ScanTupleHashTable
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md)
  - [execTuplesUnequal](../e/execTuplesUnequal.md)
  - TermTupleHashIterator
  - CHECK_FOR_INTERRUPTS
- Types used:
  - [TupleHashTable](../T/TupleHashTable.md)
  - TupleHashIterator
  - [TupleHashEntry](../T/TupleHashEntry.md)
- Called from (representative examples):
  - [ExecHashSubPlan](../E/ExecHashSubPlan.md) (multiple calls for different subplan execution scenarios)

## Notes and Other Information
- This is a static function internal to nodeSubplan.c, used exclusively for subplan hash table operations
- The function must scan the entire hash table because hash keys cannot guide the search for partial matches
- Properly handles query cancellation through CHECK_FOR_INTERRUPTS() calls during iteration
- Uses the caller-provided equality functions rather than the hashtable's internal functions to support cross-type comparisons
- Returns true immediately upon finding the first partial match, optimizing for early termination
- The TermTupleHashIterator call is only needed when breaking out of the loop early (when a match is found)

## Simplified Source

```c
static bool
findPartialMatch(TupleHashTable hashtable, TupleTableSlot *slot, FmgrInfo *eqfunctions)
{
    int numCols = hashtable->numCols;
    AttrNumber *keyColIdx = hashtable->keyColIdx;
    TupleHashIterator hashiter;
    TupleHashEntry entry;

    // Scan entire hash table for potential matches
    InitTupleHashIterator(hashtable, &hashiter);
    while ((entry = ScanTupleHashTable(hashtable, &hashiter)) != NULL) {
        CHECK_FOR_INTERRUPTS();

        // Load hash table entry into slot and compare with input tuple
        ExecStoreMinimalTuple(entry->firstTuple, hashtable->tableslot, false);
        if (!execTuplesUnequal(slot, hashtable->tableslot, numCols, keyColIdx,
                             eqfunctions, hashtable->tab_collations, hashtable->tempcxt)) {
            // Found a partial match
            TermTupleHashIterator(&hashiter);
            return true;
        }
    }

    // No partial match found
    return false;
}
```