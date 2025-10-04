# plsample_func_handler

## Location
[src/test/modules/plsample/plsample.c:93-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/plsample/plsample.c#L93-L204)

## Overview
Handles the execution of regular (non-trigger) functions in the plsample procedural language, demonstrating function introspection, argument processing, and return value handling.

## Definition
```c
static Datum plsample_func_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
`plsample_func_handler` is the core function execution handler for the plsample procedural language. This function demonstrates a complete example of how a procedural language handler can introspect PostgreSQL functions, process their arguments, and generate return values. 

The function performs several key operations:
1. **Function Introspection**: Retrieves the function's definition from the system catalogs using the function OID
2. **Source Code Access**: Extracts and displays the function's source text from pg_proc.prosrc
3. **Memory Management**: Creates a dedicated memory context for function execution
4. **Argument Processing**: Iterates through all function arguments, converts them to string representation, and logs them
5. **Return Value Handling**: For text return types, returns the function's source code; for other types, returns NULL

This implementation serves as an educational example showing how procedural language handlers interact with PostgreSQL's internal systems. It demonstrates proper use of system cache lookups, memory context management, and type conversion functions.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL macro providing access to:
  - `fcinfo`: Function call information including arguments, function OID, and execution context
  - `fcinfo->flinfo->fn_oid`: OID of the function being called
  - `fcinfo->nargs`: Number of arguments passed to the function
  - `fcinfo->args[]`: Array of function arguments

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (lookup function definition in pg_proc)
  - `HeapTupleIsValid` (validate system cache results)
  - `GETSTRUCT` (extract structure from heap tuple)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (extract specific attributes from cache)
  - [DatumGetCString](../D/DatumGetCString.md), `DirectFunctionCall1`, `textout` (text conversion)
  - `AllocSetContextCreate` (memory context creation)
  - [get_func_arg_info](../g/get_func_arg_info.md) (extract function argument metadata)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (initialize function manager info)
  - [OutputFunctionCall](../O/OutputFunctionCall.md), `InputFunctionCall` (type I/O functions)
  - [getTypeIOParam](../g/getTypeIOParam.md) (get type I/O parameters)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release system cache entries)
  - `PG_RETURN_NULL`, `PG_RETURN_DATUM` (return value macros)
- Called from:
  - [plsample_call_handler](plsample_call_handler.md) (when handling regular function calls)

## Notes and Other Information
- Located in `src/test/modules/plsample/plsample.c:93-204`
- This is a static function, only accessible within the plsample module
- The function only returns meaningful values for functions with TEXT return type; all other types result in NULL
- Demonstrates proper PostgreSQL coding patterns including memory context usage and system cache management
- Uses ereport(NOTICE) to output function source and argument information for debugging/educational purposes
- Creates a dedicated memory context named "PL/Sample function" for function-specific allocations
- Properly handles type conversion for both input arguments (for display) and return values
- Part of PostgreSQL's test infrastructure, serving as a template for procedural language implementations
- The implementation is intentionally simple and educational rather than providing full procedural language functionality

## Simplified Source

```c
static Datum plsample_func_handler(PG_FUNCTION_ARGS) {
    HeapTuple pl_tuple;
    Datum ret;
    char *source;
    bool isnull;
    FmgrInfo *arg_out_func;
    Form_pg_proc pl_struct;
    volatile MemoryContext proc_cxt = NULL;
    Oid *argtypes;
    char **argnames;
    char *argmodes;
    char *proname;
    Oid prorettype;
    FmgrInfo result_in_func;
    int numargs;

    // Fetch function's pg_proc entry
    pl_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fcinfo->flinfo->fn_oid));
    if (!HeapTupleIsValid(pl_tuple))
        elog(ERROR, "cache lookup failed for function %u", fcinfo->flinfo->fn_oid);

    // Extract function source text
    pl_struct = (Form_pg_proc) GETSTRUCT(pl_tuple);
    proname = pstrdup(NameStr(pl_struct->proname));
    ret = SysCacheGetAttr(PROCOID, pl_tuple, Anum_pg_proc_prosrc, &isnull);
    if (isnull)
        elog(ERROR, "could not find source text of function \"%s\"", proname);
    source = DatumGetCString(DirectFunctionCall1(textout, ret));
    ereport(NOTICE, (errmsg("source text of function \"%s\": %s", proname, source)));

    // Create memory context for function execution
    proc_cxt = AllocSetContextCreate(TopMemoryContext,
                                    "PL/Sample function",
                                    ALLOCSET_SMALL_SIZES);

    arg_out_func = (FmgrInfo *) palloc0(fcinfo->nargs * sizeof(FmgrInfo));
    numargs = get_func_arg_info(pl_tuple, &argtypes, &argnames, &argmodes);

    // Process and display all function arguments
    for (int i = 0; i < numargs; i++) {
        Oid argtype = pl_struct->proargtypes.values[i];
        char *value;
        HeapTuple type_tuple;
        Form_pg_type type_struct;

        type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(argtype));
        if (!HeapTupleIsValid(type_tuple))
            elog(ERROR, "cache lookup failed for type %u", argtype);

        type_struct = (Form_pg_type) GETSTRUCT(type_tuple);
        fmgr_info_cxt(type_struct->typoutput, &(arg_out_func[i]), proc_cxt);
        ReleaseSysCache(type_tuple);

        value = OutputFunctionCall(&arg_out_func[i], fcinfo->args[i].value);
        ereport(NOTICE, (errmsg("argument: %d; name: %s; value: %s",
                               i, argnames[i], value)));
    }

    // Handle return value based on type
    prorettype = pl_struct->prorettype;
    ReleaseSysCache(pl_tuple);

    // Only return meaningful value for TEXT type, NULL for others
    if (prorettype != TEXTOID)
        PG_RETURN_NULL();

    // For TEXT return type, return the function source
    HeapTuple type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(prorettype));
    if (!HeapTupleIsValid(type_tuple))
        elog(ERROR, "cache lookup failed for type %u", prorettype);

    Form_pg_type pg_type_entry = (Form_pg_type) GETSTRUCT(type_tuple);
    Oid result_typioparam = getTypeIOParam(type_tuple);

    fmgr_info_cxt(pg_type_entry->typinput, &result_in_func, proc_cxt);
    ReleaseSysCache(type_tuple);

    ret = InputFunctionCall(&result_in_func, source, result_typioparam, -1);
    PG_RETURN_DATUM(ret);
}
```