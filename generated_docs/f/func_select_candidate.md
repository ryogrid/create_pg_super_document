# func_select_candidate

## Location
[src/backend/parser/parse_func.c:1008-1394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L1008-L1394)

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
  - [getBaseType](../g/getBaseType.md) (reduces domain types to base types)
  - [TypeCategory](../T/TypeCategory.md) (determines type category for categorization)
  - [IsPreferredType](../I/IsPreferredType.md) (checks if a type is preferred in its category)
  - [get_type_category_preferred](../g/get_type_category_preferred.md) (gets both category and preference info)
  - [can_coerce_type](../c/can_coerce_type.md) (validates type coercion compatibility)
- Called from (representative examples):
  - [func_get_detail](func_get_detail.md) (from parse_func.c:1562)
  - [oper_select_candidate](../o/oper_select_candidate.md) (from parse_oper.c:342)

## Notes and Other Information
- Returns the selected candidate or NULL if no unique best match exists
- Used for both function and operator resolution (shared algorithm)
- Implements historical PostgreSQL heuristics dating back to version 4.2/1.0.x
- STRING type category receives special priority bias for unknown-type literals
- Preserves exact-match semantics while allowing appropriate coercions
- Critical component in PostgreSQL's polymorphic function resolution system
- Enforces FUNC_MAX_ARGS limit for safety against array overruns

## Simplified Source

```c
FuncCandidateList func_select_candidate(int nargs,
                                        Oid *input_typeids,
                                        FuncCandidateList candidates) {
    FuncCandidateList current_candidate, last_candidate;
    Oid input_base_typeids[FUNC_MAX_ARGS];
    int ncandidates, nbestMatch, nmatch, nunknowns;

    // Validate argument count
    if (nargs > FUNC_MAX_ARGS) {
        ereport(ERROR, /* too many arguments */);
    }

    // Reduce domain types to base types, count unknowns
    nunknowns = 0;
    for (int i = 0; i < nargs; i++) {
        if (input_typeids[i] != UNKNOWNOID) {
            input_base_typeids[i] = getBaseType(input_typeids[i]);
        } else {
            input_base_typeids[i] = UNKNOWNOID;
            nunknowns++;
        }
    }

    // Phase 1: Keep candidates with most exact type matches
    ncandidates = 0;
    nbestMatch = 0;
    last_candidate = NULL;

    for (current_candidate = candidates; current_candidate != NULL;
         current_candidate = current_candidate->next) {

        // Count exact matches
        nmatch = 0;
        for (int i = 0; i < nargs; i++) {
            if (input_base_typeids[i] != UNKNOWNOID &&
                current_candidate->args[i] == input_base_typeids[i]) {
                nmatch++;
            }
        }

        // Keep best candidates
        if (nmatch > nbestMatch || last_candidate == NULL) {
            nbestMatch = nmatch;
            candidates = current_candidate;
            last_candidate = current_candidate;
            ncandidates = 1;
        } else if (nmatch == nbestMatch) {
            last_candidate->next = current_candidate;
            last_candidate = current_candidate;
            ncandidates++;
        }
    }

    if (last_candidate) last_candidate->next = NULL;
    if (ncandidates == 1) return candidates;

    // Phase 2: Consider preferred types in same category
    TYPCATEGORY slot_category[FUNC_MAX_ARGS];
    for (int i = 0; i < nargs; i++) {
        slot_category[i] = TypeCategory(input_base_typeids[i]);
    }

    ncandidates = 0;
    nbestMatch = 0;
    last_candidate = NULL;

    for (current_candidate = candidates; current_candidate != NULL;
         current_candidate = current_candidate->next) {

        nmatch = 0;
        for (int i = 0; i < nargs; i++) {
            if (input_base_typeids[i] != UNKNOWNOID) {
                if (current_candidate->args[i] == input_base_typeids[i] ||
                    IsPreferredType(slot_category[i], current_candidate->args[i])) {
                    nmatch++;
                }
            }
        }

        // Keep best matches
        if (nmatch > nbestMatch || last_candidate == NULL) {
            nbestMatch = nmatch;
            candidates = current_candidate;
            last_candidate = current_candidate;
            ncandidates = 1;
        } else if (nmatch == nbestMatch) {
            last_candidate->next = current_candidate;
            last_candidate = current_candidate;
            ncandidates++;
        }
    }

    if (last_candidate) last_candidate->next = NULL;
    if (ncandidates == 1) return candidates;

    // Phase 3: Handle unknown types by category resolution
    if (nunknowns > 0) {
        // Try to resolve unknown types based on type categories
        // Prefer STRING category, then consistent categories
        // [Complex unknown resolution logic simplified...]

        // If successful resolution, filter candidates accordingly
    }

    // Phase 4: Last resort - assume unknowns match known types
    if (nunknowns < nargs) {
        Oid known_type = UNKNOWNOID;

        // Find if all known types are the same
        for (int i = 0; i < nargs; i++) {
            if (input_base_typeids[i] != UNKNOWNOID) {
                if (known_type == UNKNOWNOID) {
                    known_type = input_base_typeids[i];
                } else if (known_type != input_base_typeids[i]) {
                    known_type = UNKNOWNOID;
                    break;
                }
            }
        }

        if (known_type != UNKNOWNOID) {
            // Apply same type to all arguments and test
            for (int i = 0; i < nargs; i++) {
                input_base_typeids[i] = known_type;
            }

            // Find unique match with coercion
            ncandidates = 0;
            for (current_candidate = candidates; current_candidate != NULL;
                 current_candidate = current_candidate->next) {
                if (can_coerce_type(nargs, input_base_typeids,
                                   current_candidate->args, COERCION_IMPLICIT)) {
                    if (++ncandidates > 1) break;
                    last_candidate = current_candidate;
                }
            }

            if (ncandidates == 1) {
                last_candidate->next = NULL;
                return last_candidate;
            }
        }
    }

    return NULL; // No unique best candidate found
}
```