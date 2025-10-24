# tuplesort_begin_index_hash

## Location
[src/backend/utils/sort/tuplesortvariants.c:437-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L437-L489)

## Overview
Initializes a Tuplesortstate for sorting hash index tuples during hash index creation, organizing tuples by hash codes for efficient bucket distribution.

## Definition

```c
Tuplesortstate *
tuplesort_begin_index_hash(Relation heapRel,
						   Relation indexRel,
						   uint32 high_mask,
						   uint32 low_mask,
						   uint32 max_buckets,
						   int workMem,
						   SortCoordinate coordinate,
						   int sortopt)
```
## Detailed Description
This function creates a specialized tuplesort state for hash index creation operations. Unlike other index types that sort by key values, hash indexes sort tuples by their hash codes to distribute them efficiently across hash buckets. The function configures a single-key sort operation where the sort key is the computed hash code rather than the original data values. It sets up hash-specific comparison functions and stores the bucket configuration parameters needed for proper hash index organization.

## Parameters / Member Variables
- `heapRel`: The heap relation being indexed
- `indexRel`: The hash index relation being created
- `high_mask`: Bitmask for high-order hash bucket calculation
- `low_mask`: Bitmask for low-order hash bucket calculation
- `max_buckets`: Maximum number of hash buckets allowed
- `workMem`: Amount of memory (in KB) available for sorting operations
- `coordinate`: Coordination structure for parallel sorting operations
- `sortopt`: Sorting options bitmask (e.g., TUPLESORT_RANDOMACCESS)
## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_common](tuplesort_begin_common.md)
  - TuplesortstateGetPublic
  - [removeabbrev_index](../r/removeabbrev_index.md)
  - [comparetup_index_hash](../c/comparetup_index_hash.md)
  - [comparetup_index_hash_tiebreak](../c/comparetup_index_hash_tiebreak.md)
  - [writetup_index](../w/writetup_index.md)
  - [readtup_index](../r/readtup_index.md)
- Called from (representative examples):
  - [_h_spoolinit](../h/_h_spoolinit.md) (hashsort.c:83)

## Notes and Other Information
- Uses only one sort key (nKeys = 1) since sorting is based on hash codes rather than multiple data attributes
- Creates a TuplesortIndexHashArg structure to store hash-specific parameters including bucket masks
- Enables datum1 optimization since there's only one sort key
- The hash masks and max_buckets parameters control the hash bucket distribution strategy
- Used specifically during CREATE INDEX operations for hash indexes
- Does not require complex sort support setup since comparison is based on simple hash code values
- The sorting organizes tuples for efficient insertion into hash buckets during index build

## Simplified Source

```c
Tuplesortstate *
tuplesort_begin_index_hash(Relation heapRel, Relation indexRel,
                          uint32 high_mask, uint32 low_mask, uint32 max_buckets,
                          int workMem, SortCoordinate coordinate, int sortopt)
{
    // Initialize common tuplesort state
    Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate, sortopt);
    TuplesortPublic *base = TuplesortstateGetPublic(state);

    // Switch to sort context and allocate hash-specific args
    MemoryContext oldcontext = MemoryContextSwitchTo(base->maincontext);
    TuplesortIndexHashArg *arg = palloc(sizeof(TuplesortIndexHashArg));

    // Hash indexes sort by hash code only (single key)
    base->nKeys = 1;

    // Configure hash-specific function pointers
    base->removeabbrev = removeabbrev_index;
    base->comparetup = comparetup_index_hash;
    base->comparetup_tiebreak = comparetup_index_hash_tiebreak;
    base->writetup = writetup_index;
    base->readtup = readtup_index;
    base->haveDatum1 = true;
    base->arg = arg;

    // Store index relations
    arg->index.heapRel = heapRel;
    arg->index.indexRel = indexRel;

    // Store hash bucket configuration
    arg->high_mask = high_mask;
    arg->low_mask = low_mask;
    arg->max_buckets = max_buckets;

    MemoryContextSwitchTo(oldcontext);

    return state;
}
```