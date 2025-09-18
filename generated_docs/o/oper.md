# oper

## Location
src/backend/parser/parse_oper.c: 370 - 449

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
  - make_oper_cache_key (creates cache lookup key)
  - find_oper_cache_entry (checks operator cache)
  - binary_oper_exact (attempts exact type match)
  - OpernameGetCandidates (gets candidate operators by name)
  - oper_select_candidate (selects best candidate from multiple matches)
  - make_oper_cache_entry (updates cache with successful lookup)
  - op_error (reports operator resolution errors)
  - SearchSysCache1, ReleaseSysCache (syscache management)
- Called from (representative examples):
  - LookupOperWithArgs (operator lookup with argument specification)
  - compatible_oper (exact/binary-compatible operator resolution)
  - make_op (expression tree operator node creation)
  - make_scalar_array_op (scalar array operator creation)

## Notes and Other Information
- Returns NULL if noError is true and no operator found
- The returned Operator is a syscache entry that must be released by caller
- Uses caching for performance optimization of repeated lookups
- Handles InvalidOid input types by using the other operand's type
- Part of PostgreSQL's comprehensive operator overload resolution system
- Located in src/backend/parser/parse_oper.c:370-449
- Critical component used throughout the PostgreSQL system for operator resolution