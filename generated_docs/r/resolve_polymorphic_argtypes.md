# resolve_polymorphic_argtypes

## Location
[src/backend/utils/fmgr/funcapi.c:1064-1327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1064-L1327)

## Overview
Resolves polymorphic argument types (ANYELEMENT, ANYARRAY, etc.) in function signatures by deducing concrete data types from input arguments in a function call expression.

## Definition


## Detailed Description
This function replaces polymorphic type placeholders in a function's argument type array with concrete data types determined from the actual arguments provided in a function call. It handles both traditional polymorphic types (ANYELEMENT, ANYARRAY, ANYRANGE, ANYMULTIRANGE) and the newer ANYCOMPATIBLE family of types.

The function operates in two passes:
1. First pass: Processes input arguments to extract concrete types and identifies output arguments that need resolution
2. Second pass: Resolves remaining polymorphic output arguments by deducing types from the concrete types found in the first pass

The logic assumes that the parser has already enforced type consistency and coerced ANYCOMPATIBLE arguments to a common supertype. This is the same logic used by resolve_polymorphic_tupdesc but with different argument representation.

## Parameters / Member Variables
- : Number of arguments in the function signature
- : Array of argument type OIDs that may contain polymorphic types to be resolved
- : Array of argument modes (IN, OUT, INOUT, TABLE) or NULL if all are IN mode
- : Function call expression containing actual argument values for type deduction

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_expr_argtype](../g/get_call_expr_argtype.md)
  - [resolve_anyelement_from_others](resolve_anyelement_from_others.md)
  - [resolve_anyarray_from_others](resolve_anyarray_from_others.md)
  - [resolve_anyrange_from_others](resolve_anyrange_from_others.md)
  - [resolve_anymultirange_from_others](resolve_anymultirange_from_others.md)
  - [polymorphic_actuals](../p/polymorphic_actuals.md) (struct)
  - PROARGMODE_IN/OUT/TABLE constants
- Called from (representative examples):
  - TypeFuncClass (referenced in funcapi.h)

## Notes and Other Information
- Returns true if all polymorphic types can be resolved, false if necessary information is missing
- Handles both traditional polymorphic types and ANYCOMPATIBLE family types separately
- Requires valid call_expr to extract argument types; returns false if call_expr is NULL or argument types aren't identifiable
- The function modifies the argtypes array in-place, replacing polymorphic OIDs with concrete type OIDs
- Critical for PostgreSQL's polymorphic function resolution system, enabling functions to work with multiple data types while maintaining type safety