# Int64GetDatum

## Location
[src/backend/utils/fmgr/fmgr.c:1807-1815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1807-L1815)

## Overview
Int64GetDatum converts a 64-bit integer value to a PostgreSQL Datum representation, handling the memory allocation required when 64-bit integers are passed by reference.

## Definition

```c
Datum
Int64GetDatum(int64 X)
```
## Detailed Description
Int64GetDatum is a utility function that converts a 64-bit integer (int64) into PostgreSQL's universal Datum type. This function is specifically used when PostgreSQL is compiled without USE_FLOAT8_BYVAL, meaning that 64-bit values (both integers and floats) are passed by reference rather than by value. The function allocates memory using palloc() to store the integer value and returns a pointer to this memory location wrapped as a Datum.

The function is part of PostgreSQL's type system infrastructure that provides a uniform interface for handling different data types, regardless of whether they are passed by value or by reference. This abstraction allows the same code to work with different compilation configurations.

## Parameters / Member Variables
- `X`: The 64-bit integer value to be converted to a Datum
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts pointer to Datum)
- Called from (representative examples):
  - PG_RETURN_INT64 (macro for returning int64 from SQL functions)
  - Various statistical and system information functions
  - Sequence-related functions
  - Cash/money type conversion functions
  - [Range](../R/Range.md) type functions
  - File system functions

## Notes and Other Information
- This function is only compiled when USE_FLOAT8_BYVAL is not defined
- When USE_FLOAT8_BYVAL is defined, Int64GetDatumFast macro is used instead for better performance
- The allocated memory is managed by PostgreSQL's memory context system
- This function is widely used throughout PostgreSQL for returning int64 values from C functions to the SQL layer
- The function works in conjunction with DatumGetInt64() for the reverse conversion

## Simplified Source

```c
// Simplified version of Int64GetDatum
Datum Int64GetDatum(int64 X) {
    // Allocate memory to store the 64-bit integer
    int64 *retval = (int64 *) palloc(sizeof(int64));

    // Store the value in the allocated memory
    *retval = X;

    // Return the pointer as a Datum
    return PointerGetDatum(retval);
}
```

Key simplifications made:
- Added clear comments explaining each step
- Maintained the essential 3-step process: allocate, store, return
- Preserved all functionality for pass-by-reference 64-bit integers
- Note: This version is only used when USE_FLOAT8_BYVAL is not defined