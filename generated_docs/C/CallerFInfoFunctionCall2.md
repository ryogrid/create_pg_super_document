# CallerFInfoFunctionCall2

## Location
src/backend/utils/fmgr/fmgr.c: 1085 - 1111

## Overview
CallerFInfoFunctionCall2 is a utility function that calls a PostgreSQL function with 2 arguments using caller-provided function information (FmgrInfo), allowing for enhanced context and state sharing between caller and callee.

## Definition
```c
Datum CallerFInfoFunctionCall2(PGFunction func, FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
```

## Detailed Description
This function is similar to DirectFunctionCall2 but uses the flinfo parameter to initialize the function call information structure. This allows the caller to pass additional context through the FmgrInfo structure, particularly the fn_extra and fn_mcxt fields. The function is designed for scenarios where the caller wants to share state or context with the called function, such as cached data or memory contexts. The callee should primarily use fn_extra and fn_mcxt fields, as other fields typically describe the calling function.

## Parameters / Member Variables
- `func`: Pointer to the PostgreSQL function to be called
- `flinfo`: Pointer to FmgrInfo structure containing function metadata and context
- `collation`: Object ID (Oid) specifying the collation to use for the function call
- `arg1`: First argument value of type Datum
- `arg2`: Second argument value of type Datum

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info allocation)
  - InitFunctionCallInfoData (initializes function call structure with flinfo)
  - elog (error logging function)
- Called from (representative examples):
  - PG_MODULE_MAGIC (referenced in header)

## Notes and Other Information
- The flinfo parameter allows sharing of function metadata and context between caller and callee
- Recommended that callees only use fn_extra and fn_mcxt fields from flinfo
- The calling function should not have used fn_extra unless compatible with callee's usage
- Both arguments are automatically marked as non-null (isnull = false)
- The function will throw an ERROR if the called function returns NULL
- Part of a family of CallerFInfoFunctionCall functions for enhanced function calling
- Located in src/backend/utils/fmgr/fmgr.c:1085-1111