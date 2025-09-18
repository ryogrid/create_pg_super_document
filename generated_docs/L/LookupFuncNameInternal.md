# LookupFuncNameInternal

## Location
src/backend/parser/parse_func.c: 2049 - 2143

## Overview
LookupFuncNameInternal is the core workhorse function for function name lookup that handles the actual logic of finding functions, procedures, and aggregates by name and argument types, returning appropriate error codes when lookups fail.

## Definition
```c
static Oid LookupFuncNameInternal(ObjectType objtype, List *funcname,
                                 int nargs, const Oid *argtypes,
                                 bool include_out_arguments, bool missing_ok,
                                 FuncLookupError *lookupError)
```

## Detailed Description
This static function serves as the central implementation for PostgreSQL's function lookup mechanism. It searches for functions, procedures, or aggregates based on the provided name and argument signature, filtering results by object type (function/procedure/aggregate/routine). The function uses FuncnameGetCandidates to retrieve potential matches and then applies filtering based on argument types and object kind (prokind). When multiple matches are found or no matches exist, it sets appropriate error codes rather than throwing exceptions directly, allowing callers to handle errors appropriately.

## Parameters / Member Variables
- `objtype`: The type of object to search for (OBJECT_FUNCTION, OBJECT_PROCEDURE, OBJECT_AGGREGATE, or OBJECT_ROUTINE)
- `funcname`: List representing the possibly schema-qualified function name
- `nargs`: Number of arguments (-1 means unspecified arguments)
- `argtypes`: Array of argument type OIDs (can be NULL if nargs == 0)
- `include_out_arguments`: Whether to include OUT parameters in the search
- `missing_ok`: Whether to allow the lookup to fail without immediate error
- `lookupError`: Output parameter indicating the type of lookup error that occurred

## Dependencies
- Functions called/Symbols referenced:
  - FuncnameGetCandidates
  - get_func_prokind
  - FUNCLOOKUP_NOSUCHFUNC
  - FUNCLOOKUP_AMBIGUOUS
  - OidIsValid
- Called from (representative examples):
  - LookupFuncName
  - LookupFuncWithArgs

## Notes and Other Information
This function implements PostgreSQL's function overloading resolution by examining both argument types and object kinds. It distinguishes between functions, procedures, and aggregates using the prokind system catalog field. The function is designed to be called by higher-level lookup functions that handle user-facing error messages and missing_ok semantics. Error handling is deferred to callers through the lookupError parameter, allowing for customized error messages based on context.