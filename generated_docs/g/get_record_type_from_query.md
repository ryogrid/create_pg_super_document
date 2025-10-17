# get_record_type_from_query

## Location
[src/backend/utils/adt/jsonfuncs.c:3660-3696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3660-L3696)

## Overview
A static function that determines the record type from the calling query context when the target type cannot be extracted from function arguments, particularly for  functions.

## Definition

```c
static void
get_record_type_from_query(FunctionCallInfo fcinfo,
						   const char *funcname,
						   PopulateRecordCache *cache)
```
## Detailed Description
This function extracts record type information from the SQL query context when it cannot be determined from function arguments. It's primarily used for  functions and as a fallback for  functions when the first argument is a null record. The function validates that the result type is composite and sets up the necessary tuple descriptor cache.

Key behaviors:
- Uses query context to determine result type structure
- Validates that the result type is composite (not domain-over-composite)
- Prevents memory leaks by cleaning up previous tuple descriptors
- Creates a copy of the tuple descriptor in the function's memory context
- Provides helpful error messages with usage hints

## Parameters / Member Variables
- `fcinfo`: Function call information containing query context and execution details
- `*funcname`: Name of the calling function (used in error messages for clarity)
- `*cache`: PopulateRecordCache structure to be populated with type information
## Dependencies
- Functions called/Symbols referenced:
  - [get_call_result_type](get_call_result_type.md)
  - TYPEFUNC_COMPOSITE
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - ereport (for error handling)
- Called from (representative examples):
  - [populate_record_worker](../p/populate_record_worker.md)
  - [populate_recordset_worker](../p/populate_recordset_worker.md)

## Notes and Other Information
- This function is used when type information cannot be extracted from arguments
- Handles the case where the first argument is 
- Cannot handle domain-over-composite types due to syntactic limitations
- Includes memory management to prevent leaks on repeated calls
- Provides user-friendly error messages with hints for proper usage
- Part of PostgreSQL's JSON-to-record conversion infrastructure
- The error hint suggests using column definition lists in FROM clauses

## Simplified Source

```c
static void
get_record_type_from_query(FunctionCallInfo fcinfo,
                           const char *funcname,
                           PopulateRecordCache *cache)
{
    TupleDesc tupdesc;
    MemoryContext old_cxt;

    // Determine result type from query context
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("could not determine row type for result of %s",
                        funcname),
                 errhint("Provide a non-null record argument, "
                         "or call the function in the FROM clause "
                         "using a column definition list.")));

    Assert(tupdesc);
    cache->argtype = tupdesc->tdtypeid;

    // Clean up previous tuple descriptor to prevent memory leak
    if (cache->c.io.composite.tupdesc)
        FreeTupleDesc(cache->c.io.composite.tupdesc);

    // Save tuple descriptor in function memory context
    old_cxt = MemoryContextSwitchTo(cache->fn_mcxt);
    cache->c.io.composite.tupdesc = CreateTupleDescCopy(tupdesc);
    cache->c.io.composite.base_typid = tupdesc->tdtypeid;
    cache->c.io.composite.base_typmod = tupdesc->tdtypmod;
    MemoryContextSwitchTo(old_cxt);
}
```