# be_lo_get_fragment

## Location
[src/backend/libpq/be-fsstubs.c:806-826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L806-L826)

## Overview
A PostgreSQL backend function that reads a specific fragment (range) of data from a large object, returning only the requested portion as bytea data.

## Definition

```c
Datum
be_lo_get_fragment(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides selective reading capability for PostgreSQL large objects by allowing retrieval of a specific range of bytes. Unlike  which reads the entire object, this function accepts an offset and length parameter to read only a portion of the large object. It includes parameter validation to ensure the requested length is not negative, and delegates the actual reading operation to the internal  function. This functionality is essential for efficient handling of large objects where only specific portions need to be accessed.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  -  (Oid): The object identifier of the large object to read from
  -  (int64): The starting position in bytes from which to begin reading
  -  (int32): The number of bytes to read from the large object

## Dependencies
- Functions called/Symbols referenced:
  - : Internal function that performs the actual fragment reading
  - : Macro to extract OID argument from function call
  - : Macro to extract 64-bit integer argument (offset)
  - : Macro to extract 32-bit integer argument (length)
  - : Macro to return bytea result
  - : Error reporting function for parameter validation
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Validates that the requested length () is not negative before processing
- Supports reading from any offset within the large object
- Returns data as bytea type for binary content handling
- More efficient than reading entire large objects when only a portion is needed
- Part of PostgreSQL's large object filesystem stub interface
- Located in src/backend/libpq/be-fsstubs.c:806-826

## Simplified Source

```c
Datum be_lo_get_fragment(PG_FUNCTION_ARGS) {
    Oid loOid = PG_GETARG_OID(0);
    int64 offset = PG_GETARG_INT64(1);
    int32 nbytes = PG_GETARG_INT32(2);

    // Validate length parameter
    if (nbytes < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("requested length cannot be negative")));

    // Delegate to internal function for actual fragment reading
    bytea *result = lo_get_fragment_internal(loOid, offset, nbytes);

    PG_RETURN_BYTEA_P(result);
}
```