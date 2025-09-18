# func_match_argtypes

## Location
src/backend/parser/parse_func.c: 923 - 1007

## Overview
Filters a list of function candidates to those that accept the given input data types either exactly or through implicit coercion.

## Definition
```c
int func_match_argtypes(int nargs,
                       Oid *input_typeids,
                       FuncCandidateList raw_candidates,
                       FuncCandidateList *candidates)
```

## Detailed Description
func_match_argtypes performs type compatibility checking on a list of function candidates. Given candidate functions that already match by name and argument count, it determines which ones can actually accept the provided input data types. The function uses PostgreSQL's coercion system to determine type compatibility, allowing implicit type conversions where appropriate.

The function modifies the input list structure by creating a new filtered list of compatible candidates. It assumes that UNKNOWN type inputs can be coerced to anything, so functions are not eliminated based on UNKNOWN types alone.

## Parameters / Member Variables
- `nargs`: Number of input arguments to match
- `input_typeids`: Array of OID values representing the types of input arguments
- `raw_candidates`: Original list of function candidates to filter
- `candidates`: Output parameter that receives the filtered list of compatible candidates

## Dependencies
- Functions called/Symbols referenced:
  - can_coerce_type (performs the actual type coercion checking)
  - FuncCandidateList (data structure for maintaining candidate function lists)
  - COERCION_IMPLICIT (specifies implicit coercion rules)
- Called from (representative examples):
  - func_get_detail (from parse_func.c:1548)
  - oper_select_candidate (from parse_oper.c:323)

## Notes and Other Information
- Returns the number of compatible candidates found
- Uses implicit coercion rules (COERCION_IMPLICIT) for type checking
- Modifies the input list structure but preserves it if no matches are found
- Critical component in PostgreSQL's function resolution process
- UNKNOWN type inputs are treated as compatible with any target type
- Creates a new linked list of compatible candidates in reverse order