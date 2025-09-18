# func_select_candidate

## Location
src/backend/parser/parse_func.c: 1008 - 1394

## Overview
Resolves function overloading conflicts by selecting the best candidate from multiple compatible functions using PostgreSQL's type resolution heuristics.

## Definition
```c
FuncCandidateList func_select_candidate(int nargs,
                                        Oid *input_typeids,
                                        FuncCandidateList candidates)
```

## Detailed Description
func_select_candidate implements PostgreSQL's sophisticated function resolution algorithm when multiple function candidates match by name and argument count. It applies a series of increasingly permissive heuristics to select the best match:

1. **Exact Type Matching**: Prefers candidates with exact matches on argument types
2. **Preferred Type Matching**: Considers functions with preferred types in the same category as input types
3. **Unknown Type Resolution**: For unknown-type arguments, attempts to resolve them based on type categories, with bias toward STRING types
4. **Known Type Propagation**: As a last resort, assumes unknown arguments have the same type as known arguments

The function handles domain types by reducing them to base types, ensuring consistent matching. It maintains the principle of never selecting a wrong function over better alternatives.

## Parameters / Member Variables
- `nargs`: Number of function arguments to match
- `input_typeids`: Array of OID values representing input argument types
- `candidates`: Linked list of function candidates to choose from

## Dependencies
- Functions called/Symbols referenced:
  - getBaseType (reduces domain types to base types)
  - TypeCategory (determines type category for categorization)
  - IsPreferredType (checks if a type is preferred in its category)
  - get_type_category_preferred (gets both category and preference info)
  - can_coerce_type (validates type coercion compatibility)
- Called from (representative examples):
  - func_get_detail (from parse_func.c:1562)
  - oper_select_candidate (from parse_oper.c:342)

## Notes and Other Information
- Returns the selected candidate or NULL if no unique best match exists
- Used for both function and operator resolution (shared algorithm)
- Implements historical PostgreSQL heuristics dating back to version 4.2/1.0.x
- STRING type category receives special priority bias for unknown-type literals
- Preserves exact-match semantics while allowing appropriate coercions
- Critical component in PostgreSQL's polymorphic function resolution system
- Enforces FUNC_MAX_ARGS limit for safety against array overruns