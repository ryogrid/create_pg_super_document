# int2vectorrecv

## Location
[src/backend/utils/adt/int.c:231-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L231-L272)

## Overview
Converts external binary format data received over the network or from storage into PostgreSQL's internal int2vector data type.

## Definition
```c
Datum int2vectorrecv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function handles the binary input protocol for int2vector data types, converting binary data streams into the internal int2vector format. It delegates the actual parsing to the generic array_recv function but requires special handling for function call information caching. The function performs comprehensive validation to ensure the resulting data structure meets int2vector requirements: must be 1-dimensional, 0-based indexing, contain no null values, and have INT2OID element type.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `buf`: StringInfo buffer containing the binary data to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - `LOCAL_FCINFO` (local function call info macro)
  - [int2vector](int2vector.md) (data type)
  - `InitFunctionCallInfoData` (function call initialization)
  - [array_recv](../a/array_recv.md) (generic array binary input function)
  - `ARR_NDIM`, `ARR_HASNULL`, `ARR_ELEMTYPE`, `ARR_LBOUND` (array metadata macros)
  - `ereport` (error reporting)
- Called from (representative examples):
  - PostgreSQL binary protocol handlers
  - Network communication and data storage systems

## Notes and Other Information
- Cannot use DirectFunctionCall3 due to array_recv's need for function info caching
- Manually sets up local function call info with appropriate parameters
- Validates that the result is a proper int2vector (1-D, 0-based, no nulls, INT2OID elements)
- Uses -1 as the typmod parameter to indicate no specific type modifier
- Essential for binary protocol communication and data serialization/deserialization