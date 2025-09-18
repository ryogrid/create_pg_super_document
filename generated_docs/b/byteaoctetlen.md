# byteaoctetlen

## Location
src/backend/utils/adt/varlena.c: 2922 - 2937

## Overview
A PostgreSQL function that returns the number of data bytes contained in a bytea (binary data) value, excluding the variable-length header overhead.

## Definition
```c
Datum byteaoctetlen(PG_FUNCTION_ARGS)
```

## Detailed Description
The `byteaoctetlen` function calculates and returns the actual number of data bytes stored in a bytea instance. It works by getting the total raw size of the datum (including header) using `toast_raw_datum_size` and subtracting the variable-length header size (`VARHDRSZ`, which is 4 bytes for a 32-bit integer).

A key optimization of this function is that it does not need to detoast the input datum to determine its length. This makes it very efficient for determining bytea lengths without the overhead of potentially decompressing or fetching externally stored (TOASTed) data. The function works directly with the datum's header information to calculate the content size.

## Parameters / Member Variables
- `str`: The input bytea datum (retrieved using `PG_GETARG_DATUM(0)`) whose length is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - [toast_raw_datum_size](../t/toast_raw_datum_size.md)
  - PG_GETARG_DATUM
  - PG_RETURN_INT32
  - VARHDRSZ (constant defined as sizeof(int32))
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's function call infrastructure)

## Notes and Other Information
- Optimized implementation that avoids detoasting the input datum
- Returns only the actual data bytes, excluding the 4-byte variable-length header (`VARHDRSZ`)
- Works efficiently with both normal and TOASTed (compressed/external) bytea values
- Located in `src/backend/utils/adt/varlena.c` at lines 2922-2937
- Part of PostgreSQL's bytea data type support functions
- Essential for determining binary data sizes without performance penalties
- Equivalent to the `LENGTH()` function when applied to bytea values
- The function name reflects "octet length" (byte length) terminology