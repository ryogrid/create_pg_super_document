# gtsvector_consistent

## Location
[src/backend/utils/adt/tsgistidx.c:334-373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L334-L373)

## Overview
The main consistency checking function for TSVector GiST indexing that determines whether an index entry matches a given TSQuery by delegating to appropriate sub-functions based on data type.

## Definition
```c
Datum gtsvector_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the primary entry point for consistency checking in TSVector GiST indexes. It implements the GiST consistent method, which is responsible for determining whether an index entry (either leaf or non-leaf) satisfies a given query condition.

The function employs a two-tier strategy based on the type of data stored in the index entry:

1. **Signature-based matching** (for non-leaf pages): Uses bit signatures and calls `checkcondition_bit` for probabilistic matching via bloom filter logic.

2. **Array-based matching** (for leaf pages): Uses sorted hash arrays and calls `checkcondition_arr` for direct hash-based lookup.

The function handles special cases such as empty queries (returns false) and ALLISTRUE signatures (returns true). All matches are marked as requiring recheck since both hash-based and signature-based matching can produce false positives.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `entry`: GISTENTRY pointer to the index entry being checked
  - `query`: TSQuery containing the search conditions
  - `strategy`: Strategy number (unused, commented out)
  - `subtype`: Subtype OID (unused, commented out)  
  - `recheck`: Pointer to boolean flag indicating if recheck is needed

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TSQUERY`: Extracts TSQuery from function arguments
  - [DatumGetPointer](../D/DatumGetPointer.md): Converts Datum to pointer
  - `ISSIGNKEY`: Checks if entry contains signature data
  - `ISALLTRUE`: Checks if signature has all bits set
  - `TS_execute`: Executes tsquery matching with callback functions
  - `GETQUERY`: Extracts query tree from TSQuery
  - [checkcondition_bit](../c/checkcondition_bit.md): Callback for signature-based matching
  - [checkcondition_arr](../c/checkcondition_arr.md): Callback for array-based matching
  - `GETARR`: Gets array data from SignTSVector
  - `ARRNELEM`: Gets number of array elements
- Called from (representative examples):
  - [gtsvector_consistent_oldsig](gtsvector_consistent_oldsig.md): Wrapper for backward compatibility
  - GiST index operations during query processing

## Notes and Other Information
- Always sets `*recheck = true` because both matching strategies can produce false positives
- Implements the core logic that differentiates between signature-based (non-leaf) and array-based (leaf) index entries
- Returns false immediately for empty queries as an optimization
- Returns true immediately for ALLISTRUE signatures as they match everything
- Uses `TS_EXEC_PHRASE_NO_POS` flag indicating phrase position is not tracked
- Part of the standard GiST operator class interface for TSVector full-text search indexing
- Critical function in PostgreSQL full-text search performance, enabling efficient query filtering across index tree levels