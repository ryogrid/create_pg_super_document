# oper

## Location
[src/backend/parser/parse_oper.c:370-449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L370-L449)

## Overview
The main function for searching and resolving binary operators in PostgreSQL, returning an operator that is coercion-compatible with the input data types.

## Definition
```c
Operator oper(ParseState *pstate, List *opname, Oid ltypeId, Oid rtypeId, bool noError, int location)
```

## Detailed Description
The `oper` function is the primary interface for binary operator resolution in PostgreSQL's parser. It implements a multi-stage search strategy: first checking a lookaside cache for performance, then attempting an exact match via `binary_oper_exact`, and finally falling back to candidate selection using `oper_select_candidate`. The function only guarantees coercion-compatibility, not exact type matching. It manages syscache entries and provides detailed error reporting when operators cannot be found. The returned operator (if any) must be released by the caller using ReleaseSysCache().

## Parameters / Member Variables
- `pstate`: Parse state context for error reporting (can be NULL)
- `opname`: List containing the operator name components (namespace, operator symbol)
- `ltypeId`: Object identifier of the left operand's data type
- `rtypeId`: Object identifier of the right operand's data type  
- `noError`: If true, return NULL on failure; if false, raise an error
- `location`: Source location for error reporting (-1 if not available)

## Dependencies
- Functions called/Symbols referenced:
  - [make_oper_cache_key](../m/make_oper_cache_key.md) (creates cache lookup key)
  - [find_oper_cache_entry](../f/find_oper_cache_entry.md) (checks operator cache)
  - [binary_oper_exact](../b/binary_oper_exact.md) (attempts exact type match)
  - [OpernameGetCandidates](../O/OpernameGetCandidates.md) (gets candidate operators by name)
  - [oper_select_candidate](oper_select_candidate.md) (selects best candidate from multiple matches)
  - [make_oper_cache_entry](../m/make_oper_cache_entry.md) (updates cache with successful lookup)
  - [op_error](op_error.md) (reports operator resolution errors)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (syscache management)
- Called from (representative examples):
  - [LookupOperWithArgs](../L/LookupOperWithArgs.md) (operator lookup with argument specification)
  - [compatible_oper](../c/compatible_oper.md) (exact/binary-compatible operator resolution)
  - [make_op](../m/make_op.md) (expression tree operator node creation)
  - [make_scalar_array_op](../m/make_scalar_array_op.md) (scalar array operator creation)

## Notes and Other Information
- Returns NULL if noError is true and no operator found
- The returned Operator is a syscache entry that must be released by caller
- Uses caching for performance optimization of repeated lookups
- Handles InvalidOid input types by using the other operand's type
- Part of PostgreSQL's comprehensive operator overload resolution system
- Located in src/backend/parser/parse_oper.c:370-449
- Critical component used throughout the PostgreSQL system for operator resolution