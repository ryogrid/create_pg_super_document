# HeapTupleGetDatum

## Location
[src/include/funcapi.h:230-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/funcapi.h#L230-L235)

## Overview
Converts a HeapTupleData pointer to a Datum, providing a convenient wrapper around HeapTupleHeaderGetDatum for complete tuple structures.

## Definition

```c
static inline Datum
HeapTupleGetDatum(const HeapTupleData *tuple)
```
## Detailed Description
HeapTupleGetDatum is a static inline function that serves as a convenience wrapper for converting a complete HeapTupleData structure to a Datum. It extracts the tuple header (t_data) from the HeapTupleData and delegates to HeapTupleHeaderGetDatum to perform the actual conversion. This function ensures that any external TOAST references within the tuple are flattened into inline values, making the resulting Datum suitable for return from functions or storage in composite structures.

The function is commonly used in PostgreSQL's function API when returning tuple results from C functions, particularly in set-returning functions (SRFs) and functions that work with composite types.

## Parameters / Member Variables
- `*tuple`: A pointer to a const HeapTupleData structure representing the complete tuple to be converted to a Datum
## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleHeaderGetDatum](HeapTupleHeaderGetDatum.md)
  - [HeapTupleData](HeapTupleData.md) (struct type)
- Called from (representative examples):
  - TupleGetDatum
  - [pg_last_committed_xact](../p/pg_last_committed_xact.md)
  - [pg_xact_commit_timestamp_origin](../p/pg_xact_commit_timestamp_origin.md)
  - [ExecEvalRow](../E/ExecEvalRow.md)
  - [ExecEvalConvertRowtype](../E/ExecEvalConvertRowtype.md)
  - Various PostgreSQL system functions returning composite types

## Notes and Other Information
- This is a static inline function defined in funcapi.h, making it highly efficient with no function call overhead
- The function provides type safety by accepting HeapTupleData* rather than HeapTupleHeader directly
- Widely used throughout PostgreSQL's codebase in system functions, executor operations, and procedural language implementations
- Essential for the function API when returning composite/record types from C functions
- The underlying HeapTupleHeaderGetDatum handles TOAST decompression automatically

## Simplified Source

```c
static inline Datum
HeapTupleGetDatum(const HeapTupleData *tuple)
{
    return HeapTupleHeaderGetDatum(tuple->t_data);
}
```