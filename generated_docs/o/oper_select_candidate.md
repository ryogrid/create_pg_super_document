# oper_select_candidate

## Location
[src/backend/parser/parse_oper.c:312-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L312-L369)

## Overview
A static function that resolves conflicts when multiple operator candidates match the input argument types by selecting the best candidate using PostgreSQL's type resolution heuristics.

## Definition

```c
struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatypes.  Do not use this if
 * you need an exact- or binary-compatible match;
```
## Detailed Description
The `oper_select_candidate` function attempts to resolve conflicts when multiple operator candidates are available for given input argument types. It first filters out candidates that cannot accept the input types (either directly or through coercion) using `func_match_argtypes`. If exactly one candidate remains, it's selected. If multiple candidates remain, the function applies the same disambiguation heuristics used for function resolution via `func_select_candidate`. The function assumes no exact match exists (as determined by the caller) and focuses on finding the best approximate match.

## Parameters / Member Variables
- `nargs`: Number of arguments for the operator
- `input_typeids`: Array of Oid values representing the input argument types
- `candidates`: List of candidate operators that could potentially match
- `operOid`: Output parameter that receives the Oid of the selected operator

## Dependencies
- Functions called/Symbols referenced:
  - [func_match_argtypes](../f/func_match_argtypes.md) (filters incompatible candidates)
  - [func_select_candidate](../f/func_select_candidate.md) (applies disambiguation heuristics)
  - FuncCandidateList (candidate list data structure)
  - FUNCDETAIL_NOTFOUND, FUNCDETAIL_NORMAL, FUNCDETAIL_MULTIPLE (result codes)
- Called from (representative examples):
  - [oper](oper.md) (main operator resolution function)
  - [left_oper](../l/left_oper.md) (left unary operator resolution)

## Notes and Other Information
- Returns FuncDetailCode indicating success, failure, or ambiguity
- Sets *operOid to InvalidOid when no suitable candidate is found
- Uses the same resolution logic as function overload resolution
- Part of PostgreSQL's operator overload resolution system
- Located in src/backend/parser/parse_oper.c:312-369