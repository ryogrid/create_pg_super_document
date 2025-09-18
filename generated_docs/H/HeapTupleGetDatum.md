# HeapTupleGetDatum

## Location
src/include/funcapi.h: 230 - 235

## Overview
Converts a HeapTupleData pointer to a Datum, providing a convenient wrapper around HeapTupleHeaderGetDatum for complete tuple structures.

## Definition


## Detailed Description
HeapTupleGetDatum is a static inline function that serves as a convenience wrapper for converting a complete HeapTupleData structure to a Datum. It extracts the tuple header (t_data) from the HeapTupleData and delegates to HeapTupleHeaderGetDatum to perform the actual conversion. This function ensures that any external TOAST references within the tuple are flattened into inline values, making the resulting Datum suitable for return from functions or storage in composite structures.

The function is commonly used in PostgreSQL's function API when returning tuple results from C functions, particularly in set-returning functions (SRFs) and functions that work with composite types.

## Parameters / Member Variables
- : A pointer to a const HeapTupleData structure representing the complete tuple to be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetDatum
  - HeapTupleData (struct type)
- Called from (representative examples):
  - TupleGetDatum
  - pg_last_committed_xact
  - pg_xact_commit_timestamp_origin
  - ExecEvalRow
  - ExecEvalConvertRowtype
  - Various PostgreSQL system functions returning composite types

## Notes and Other Information
- This is a static inline function defined in funcapi.h, making it highly efficient with no function call overhead
- The function provides type safety by accepting HeapTupleData* rather than HeapTupleHeader directly
- Widely used throughout PostgreSQL's codebase in system functions, executor operations, and procedural language implementations
- Essential for the function API when returning composite/record types from C functions
- The underlying HeapTupleHeaderGetDatum handles TOAST decompression automatically