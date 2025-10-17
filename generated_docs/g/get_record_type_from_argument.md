# get_record_type_from_argument

## Location
[src/backend/utils/adt/jsonfuncs.c:3634-3659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3634-L3659)

## Overview
A static function that extracts and validates the record type from the first argument of JSON populate functions, setting up the necessary cache structures for type handling.

## Definition

```c
static void
get_record_type_from_argument(FunctionCallInfo fcinfo,
							  const char *funcname,
							  PopulateRecordCache *cache)
```
## Detailed Description
This function performs type setup and validation for JSON populate record functions. It extracts the type information from the first function argument and prepares the column cache for efficient type handling. The function ensures that the provided argument is a valid row type (composite type or composite domain), throwing an error if the type is incompatible.

Key behaviors:
- Extracts argument type using expression context
- Initializes column cache with type information
- Validates that the argument is a composite type or composite domain
- Provides descriptive error messages for invalid argument types

## Parameters / Member Variables
- `fcinfo`: Function call information containing argument details and execution context
- `*funcname`: Name of the calling function (used in error messages for clarity)
- `*cache`: PopulateRecordCache structure to be initialized with type information
## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_argtype](get_fn_expr_argtype.md)
  - [prepare_column_cache](../p/prepare_column_cache.md)
  - TYPECAT_COMPOSITE
  - TYPECAT_COMPOSITE_DOMAIN
  - ereport (for error handling)
- Called from (representative examples):
  - [populate_record_worker](../p/populate_record_worker.md)
  - [populate_recordset_worker](../p/populate_recordset_worker.md)

## Notes and Other Information
- This is a static helper function used by JSON population functions
- Handles the common pattern of extracting record type from function arguments
- The function specifically checks for composite types and composite domains
- Error reporting includes the function name to provide context to users
- Sets up caching infrastructure to optimize repeated type operations
- Part of the argument validation and setup phase for JSON record population

## Simplified Source

```c
static void
get_record_type_from_argument(FunctionCallInfo fcinfo,
                              const char *funcname,
                              PopulateRecordCache *cache)
{
    // Extract type information from first function argument
    cache->argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);

    // Prepare column cache for efficient type handling
    prepare_column_cache(&cache->c,
                         cache->argtype, -1,
                         cache->fn_mcxt, false);

    // Validate that argument is a row type (composite or composite domain)
    if (cache->c.typcat != TYPECAT_COMPOSITE &&
        cache->c.typcat != TYPECAT_COMPOSITE_DOMAIN)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("first argument of %s must be a row type",
                        funcname)));
}
```