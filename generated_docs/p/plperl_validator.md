# plperl_validator

## Location
[src/pl/plperl/plperl.c:1989-2066](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1989-L2066)

## Overview
Validates PL/Perl function definitions during CREATE FUNCTION, checking argument and return types, and optionally compiling the function body for syntax errors.

## Definition

```c
Datum
plperl_validator(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs validation of PL/Perl functions when they are created or modified using CREATE FUNCTION or ALTER FUNCTION statements. It examines the function's metadata in pg_proc to validate that the function's signature is compatible with PL/Perl restrictions. The validator checks that return types and argument types are supported by PL/Perl, specifically disallowing most pseudotypes except for triggers, event triggers, records, and void. When check_function_bodies is enabled, it also compiles the function body to detect syntax errors early during function creation rather than at runtime.

## Parameters / Member Variables
- Implicit  parameter (accessed via PG_GETARG_OID): Object ID of the function being validated

## Dependencies
- Functions called/Symbols referenced:
  - [CheckFunctionValidatorAccess](../C/CheckFunctionValidatorAccess.md) (security check for validator access)
  - [SearchSysCache1](../S/SearchSysCache1.md) (look up function in pg_proc catalog)
  - HeapTupleIsValid (validate tuple from catalog lookup)
  - Form_pg_proc (pg_proc tuple structure)
  - [get_typtype](../g/get_typtype.md) (get PostgreSQL type category)
  - TYPTYPE_PSEUDO (pseudotype category constant)
  - TRIGGEROID, EVENT_TRIGGEROID, RECORDOID, VOIDOID (type OID constants)
  - [get_func_arg_info](../g/get_func_arg_info.md) (extract function argument information)
  - [format_type_be](../f/format_type_be.md) (format type name for error messages)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release catalog cache entry)
  - [compile_plperl_function](../c/compile_plperl_function.md) (compile function body for validation)
  - PG_RETURN_VOID (return void result)
- Called from (representative examples):
  - [plperlu_validator](plperlu_validator.md)

## Notes and Other Information
- Validates function signatures during CREATE FUNCTION and ALTER FUNCTION
- Prevents creation of functions with unsupported PostgreSQL data types
- Allows trigger functions (return type TRIGGER) and event trigger functions (return type EVENT_TRIGGER)
- Allows RECORD and VOID return types but disallows other pseudotypes
- Blocks pseudotype arguments except for RECORD type
- Performs optional function body compilation when check_function_bodies GUC is enabled
- Uses PostgreSQL's system cache to look up function metadata efficiently
- Essential for preventing runtime errors by catching type mismatches at function definition time
- Returns void as validators don't produce meaningful return values

## Simplified Source

```c
Datum plperl_validator(PG_FUNCTION_ARGS)
{
    Oid funcoid = PG_GETARG_OID(0);
    HeapTuple tuple;
    Form_pg_proc proc;
    char functyptype;
    int numargs;
    Oid *argtypes;
    char **argnames;
    char *argmodes;
    bool is_trigger = false;
    bool is_event_trigger = false;
    int i;

    // Check validator access permissions
    if (!CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid))
        PG_RETURN_VOID();

    // Look up function in pg_proc catalog
    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for function %u", funcoid);

    proc = (Form_pg_proc) GETSTRUCT(tuple);
    functyptype = get_typtype(proc->prorettype);

    // Validate return type - disallow unsupported pseudotypes
    if (functyptype == TYPTYPE_PSEUDO)
    {
        if (proc->prorettype == TRIGGEROID)
            is_trigger = true;
        else if (proc->prorettype == EVENT_TRIGGEROID)
            is_event_trigger = true;
        else if (proc->prorettype != RECORDOID && proc->prorettype != VOIDOID)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("PL/Perl functions cannot return type %s",
                                  format_type_be(proc->prorettype))));
    }

    // Validate argument types - disallow unsupported pseudotypes
    numargs = get_func_arg_info(tuple, &argtypes, &argnames, &argmodes);
    for (i = 0; i < numargs; i++)
    {
        if (get_typtype(argtypes[i]) == TYPTYPE_PSEUDO && argtypes[i] != RECORDOID)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("PL/Perl functions cannot accept type %s",
                                  format_type_be(argtypes[i]))));
    }

    ReleaseSysCache(tuple);

    // Optionally compile function body for syntax validation
    if (check_function_bodies)
    {
        (void) compile_plperl_function(funcoid, is_trigger, is_event_trigger);
    }

    PG_RETURN_VOID();
}
```