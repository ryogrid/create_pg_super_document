# FuncnameGetCandidates

## Location
[src/backend/catalog/namespace.c:1192-1584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L1192-L1584)

## Overview
Retrieves a list of function candidates that match a given function name and argument criteria, supporting various call conventions and argument matching strategies.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, &funcname);
```
## Detailed Description
FuncnameGetCandidates is a comprehensive function lookup mechanism that finds all PostgreSQL functions matching specified criteria. It supports both qualified and unqualified function names, handles positional and named argument calls, and can expand variadic functions and default arguments. The function performs namespace-aware searches, returning candidates from either a specific schema or all schemas in the search path.

The function handles complex argument matching scenarios including variadic functions, default parameter expansion, named/mixed notation calls, and OUT parameter considerations. It implements sophisticated conflict resolution to handle cases where multiple functions could match the same call signature, preferring non-variadic over variadic functions and earlier namespace entries over later ones.

## Parameters / Member Variables
- : List containing the possibly-qualified function name (schema.function or just function)
- : Number of arguments in the call (-1 means return all functions regardless of argument count)
- : List of argument names for named/mixed notation calls (NIL for positional calls)
- : Whether to consider variadic functions with matching or fewer arguments
- : Whether to consider functions that could match with default argument insertion
- : Whether OUT-mode arguments should be included in argument matching
- : Whether to return NULL instead of erroring for missing schemas or no matches

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - SearchSysCacheList1
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [MatchNamedCall](../M/MatchNamedCall.md)
  - ReleaseSysCacheList
- Called from (representative examples):
  - [func_get_detail](../f/func_get_detail.md)
  - [LookupFuncNameInternal](../L/LookupFuncNameInternal.md)
  - [regprocin](../r/regprocin.md)
  - [regprocout](../r/regprocout.md)
  - [regprocedurein](../r/regprocedurein.md)

## Notes and Other Information
- Returns FuncCandidateList which is a linked list of candidate functions
- Implements namespace masking where functions in earlier search path positions hide identically-named functions in later positions
- Handles ambiguous matches by returning a single entry with oid = 0 to represent conflicting candidates
- The returned candidate list guarantees no duplicate argument lists, but may contain multiple functions that expand to the same signature
- Supports PostgreSQL's complex function overloading and resolution rules
- Critical component of PostgreSQL's function resolution system used throughout the parser and type system