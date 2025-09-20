# pg_sequence_parameters

## Location
[src/backend/commands/sequence.c:1741-1784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L1741-L1784)

## Overview
A SQL-callable function that returns sequence parameters as a composite tuple, primarily used by system views and information schema to expose sequence metadata to users.

## Definition

```c
Datum
pg_sequence_parameters(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that provides programmatic access to sequence parameters stored in the pg_sequence system catalog. It performs permission checks and returns all sequence parameters as a structured tuple that can be used in SQL queries.

The function enforces access control by checking that the calling user has at least SELECT, UPDATE, or USAGE privileges on the sequence. This ensures that sequence parameters are only accessible to authorized users.

The returned tuple contains seven fields representing all sequence parameters:
1. **start_value** (bigint): The starting value of the sequence
2. **minimum_value** (bigint): The minimum value the sequence can reach
3. **maximum_value** (bigint): The maximum value the sequence can reach  
4. **increment** (bigint): The step size for value generation
5. **cycle_option** (boolean): Whether the sequence cycles when reaching bounds
6. **cache_size** (bigint): Number of values cached in memory
7. **data_type** (oid): The data type OID of the sequence

## Parameters / Member Variables
- Function takes a single OID argument representing the sequence relation ID (accessed via PG_GETARG_OID(0))
- Returns a Datum representing a heap tuple with sequence parameters

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - [pg_class_aclcheck](pg_class_aclcheck.md)
  - [GetUserId](../G/GetUserId.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [get_call_result_type](../g/get_call_result_type.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [Int64GetDatum](../I/Int64GetDatum.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
- Called from:
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is designed to be called from SQL as a system function
- Access control is enforced using the standard PostgreSQL ACL system with SELECT, UPDATE, or USAGE privileges required
- The function was originally created for use by the information schema but can be used by any authorized client
- Proper error handling includes permission denied errors and cache lookup failures
- Memory management follows PostgreSQL conventions with system cache tuple release
- Located in src/backend/commands/sequence.c:1741-1784