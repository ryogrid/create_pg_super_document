# DirectFunctionCall7Coll

## Location
[src/backend/utils/fmgr/fmgr.c:947-980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L947-L980)

## Overview
DirectFunctionCall7Coll is a utility function that directly calls a PostgreSQL function with 7 arguments and a specified collation, handling function call setup and result validation.

## Definition

```c
Datum
DirectFunctionCall7Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2,
						Datum arg3, Datum arg4, Datum arg5,
						Datum arg6, Datum arg7)
```
## Detailed Description
This function provides a convenient way to directly invoke PostgreSQL functions that require exactly 7 arguments with collation support. It automatically sets up the function call information structure (FunctionCallInfoData), populates it with the provided arguments (marking all as non-null), executes the function, and validates that the result is not NULL. The function is part of PostgreSQL's function manager (fmgr) system that handles dynamic function calls.

## Parameters / Member Variables
- `func`: Pointer to the PostgreSQL function to be called
- `collation`: Object ID (Oid) specifying the collation to use for the function call
- `arg1`: First argument value of type Datum
- `arg2`: Second argument value of type Datum
- `arg3`: Third argument value of type Datum
- `arg4`: Fourth argument value of type Datum
- `arg5`: Fifth argument value of type Datum
- `arg6`: Sixth argument value of type Datum
- `arg7`: Seventh argument value of type Datum
## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info allocation)
  - InitFunctionCallInfoData (initializes function call structure)
  - elog (error logging function)
- Called from (representative examples):
  - PG_MODULE_MAGIC (referenced in header)
  - DirectFunctionCall7 (related function without collation)

## Notes and Other Information
- All arguments are automatically marked as non-null (isnull = false)
- The function will throw an ERROR if the called function returns NULL
- This is part of a family of DirectFunctionCallNColl functions for different argument counts
- The collation parameter allows for locale-aware string operations
- Located in src/backend/utils/fmgr/fmgr.c:947-980