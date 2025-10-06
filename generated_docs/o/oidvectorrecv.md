# oidvectorrecv

## Location
[src/backend/utils/adt/oid.c:184-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L184-L225)

## Overview
Binary receive function that converts external binary format data into PostgreSQL's internal oidvector format.

## Definition

```c
Datum
oidvectorrecv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a type receive function that handles the conversion from PostgreSQL's external binary protocol format to the internal oidvector data structure. This function is part of the binary I/O infrastructure used for network communication and binary data exchange.

The function works by delegating to the general  function with specific parameters for oidvector (OIDOID element type, -1 typmod). However, it cannot use DirectFunctionCall3 because array_recv needs to cache data in the function info structure. After receiving the array, it performs comprehensive sanity checks to ensure the result is a valid oidvector (1-dimensional, 0-based, no nulls, correct element type).

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides:
  - : StringInfo buffer containing the binary data to parse

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info)
  - oidvector (data type)
  - InitFunctionCallInfoData (function call setup)
  - [array_recv](../a/array_recv.md) (general array receive function)
  - ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_LBOUND (array metadata macros)
  - ereport (error reporting)
- Called from (representative examples):
  - PostgreSQL binary protocol handling
  - Network communication during data transfer
  - Binary format data import operations

## Notes and Other Information
- Cannot use DirectFunctionCall3 due to array_recv's need for function info caching
- Manually sets up function call info with proper parameters for oidvector
- Performs strict validation: must be 1-dimensional, 0-based, no nulls, OIDOID elements
- Raises ERROR with ERRCODE_INVALID_BINARY_REPRESENTATION for invalid data
- Part of PostgreSQL's binary I/O protocol for efficient data transfer
- Uses Assert to verify the function call succeeded (result is not null)

## Simplified Source

```c
Datum oidvectorrecv(PG_FUNCTION_ARGS) {
    LOCAL_FCINFO(locfcinfo, 3);
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    oidvector *result;

    // Set up function call info for array_recv (needed for caching)
    InitFunctionCallInfoData(*locfcinfo, fcinfo->flinfo, 3,
                            InvalidOid, NULL, NULL);

    // Set arguments for array_recv: buffer, element type (OIDOID), typmod (-1)
    locfcinfo->args[0].value = PointerGetDatum(buf);
    locfcinfo->args[0].isnull = false;
    locfcinfo->args[1].value = ObjectIdGetDatum(OIDOID);
    locfcinfo->args[1].isnull = false;
    locfcinfo->args[2].value = Int32GetDatum(-1);
    locfcinfo->args[2].isnull = false;

    // Call array_recv to parse binary data
    result = (oidvector *) DatumGetPointer(array_recv(locfcinfo));
    Assert(!locfcinfo->isnull);

    // Validate oidvector constraints: 1-D, 0-based, no nulls, OIDOID elements
    if (ARR_NDIM(result) != 1 ||
        ARR_HASNULL(result) ||
        ARR_ELEMTYPE(result) != OIDOID ||
        ARR_LBOUND(result)[0] != 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("invalid oidvector data")));

    return PG_RETURN_POINTER(result);
}
```