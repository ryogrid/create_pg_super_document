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

## Simplified Source

```c
Datum pg_sequence_parameters(PG_FUNCTION_ARGS) {
    Oid relid = PG_GETARG_OID(0);
    TupleDesc tupdesc;
    Datum values[7];
    bool isnull[7];
    HeapTuple pgstuple;
    Form_pg_sequence pgsform;

    // Check permissions: user needs SELECT, UPDATE, or USAGE on sequence
    if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", get_rel_name(relid))));

    // Validate return type is composite
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Initialize null flags
    memset(isnull, 0, sizeof(isnull));

    // Look up sequence parameters from system catalog
    pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(pgstuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);
    pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);

    // Build result tuple with sequence parameters
    values[0] = Int64GetDatum(pgsform->seqstart);      // start_value
    values[1] = Int64GetDatum(pgsform->seqmin);        // minimum_value
    values[2] = Int64GetDatum(pgsform->seqmax);        // maximum_value
    values[3] = Int64GetDatum(pgsform->seqincrement);  // increment
    values[4] = BoolGetDatum(pgsform->seqcycle);       // cycle_option
    values[5] = Int64GetDatum(pgsform->seqcache);      // cache_size
    values[6] = ObjectIdGetDatum(pgsform->seqtypid);   // data_type

    ReleaseSysCache(pgstuple);
    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}
```