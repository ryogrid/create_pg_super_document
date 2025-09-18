# byteane

## Location
src/backend/utils/adt/varlena.c: 3826 - 3857

## Overview
A PostgreSQL function that performs inequality comparison between two bytea (binary string) values, returning true if they are different.

## Definition
```c
Datum byteane(PG_FUNCTION_ARGS)
```

## Detailed Description
The `byteane` function implements the inequality operator (!=) for bytea data type in PostgreSQL. It compares two binary string values to determine if they are not equal. Like `byteaeq`, it includes a performance optimization by first comparing the lengths of the two values using `toast_raw_datum_size()`. If the lengths differ, it immediately returns true (not equal) without needing to detoast the values. If the lengths are equal, it detoasts both values and performs a byte-by-byte comparison using `memcmp()`, returning true if any bytes differ.

The function follows the same memory-efficient design as other bytea comparison functions, ensuring proper cleanup to prevent memory leaks in btree index operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides:
  - `arg1`: First bytea value (as Datum)
  - `arg2`: Second bytea value (as Datum)

## Dependencies
- Functions called/Symbols referenced:
  - `toast_raw_datum_size`: Gets the size of a potentially toasted datum
  - `DatumGetByteaPP`: Converts Datum to bytea pointer with detoasting
  - `VARDATA_ANY`: Macro to get variable-length data portion
  - `memcmp`: Standard C library function for memory comparison
  - `PG_FREE_IF_COPY`: Macro to free memory if value was copied during detoasting
  - `PG_RETURN_BOOL`: Macro to return boolean result

- Called from (representative examples):
  - Used as the inequality operator function for bytea type in SQL operations
  - Referenced by the PostgreSQL type system for bytea comparisons

## Notes and Other Information
- Optimized for performance by checking lengths first before detoasting
- Memory-safe implementation that prevents memory leaks in btree index operations
- Complement function to `byteaeq` - returns the logical opposite result
- Part of the bytea comparison function family in varlena.c
- Uses PostgreSQL's internal memory management macros for proper cleanup
- Located in src/backend/utils/adt/varlena.c:3826-3857