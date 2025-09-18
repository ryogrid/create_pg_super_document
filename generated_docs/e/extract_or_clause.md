# extract_or_clause

## Location
src/backend/optimizer/util/orclauses.c: 156 - 253

## Overview
Extracts a restriction clause that mentions only a specific relation from a given join OR-clause by recursively processing OR/AND structures.

## Definition
```c
static Expr *extract_or_clause(RestrictInfo *or_rinfo, RelOptInfo *rel)
```

## Detailed Description
This recursive function attempts to extract restriction clauses for a specific relation from complex OR-of-AND join clauses. The function must successfully extract at least one qualifying subclause from each arm of the input OR clause to produce a valid result.

The extraction process handles several nested structures:
1. **Simple OR arms**: Direct RestrictInfo nodes that can be checked for safety
2. **AND arms**: Conjunctive subclauses where individual conjuncts are examined
3. **Nested OR arms**: Recursive OR structures that require recursive processing

The function preserves AND/OR logical flatness by ensuring that OR nodes don't appear directly underneath other OR nodes. When multiple subclauses are found in an arm, they are combined with AND logic. The final result combines all valid arms with OR logic.

A key aspect is the handling of RestrictInfo nodes: the function uses embedded RestrictInfo metadata for efficient safety checking but strips these nodes from the returned expression tree, allowing fresh RestrictInfo nodes to be built with appropriate caching for restriction clause contexts.

## Parameters / Member Variables
- `or_rinfo`: RestrictInfo containing the input OR clause to be processed
- `rel`: RelOptInfo representing the target relation for clause extraction

## Dependencies
- Functions called/Symbols referenced:
  - is_orclause
  - BoolExpr (struct access)
  - is_andclause  
  - restriction_is_or_clause
  - extract_or_clause (recursive call)
  - is_safe_restriction_clause_for
  - make_ands_explicit
  - list_concat
  - make_orclause
- Called from (representative examples):
  - extract_restriction_or_clauses
  - extract_or_clause (recursive)

## Notes and Other Information
- This is a static function only used within the orclauses.c module
- The function is recursive and handles arbitrarily nested OR/AND structures
- Returns NULL if any OR arm fails to produce extractable clauses for the target relation
- Strips RestrictInfo nodes from the result to allow proper re-caching in restriction contexts
- Maintains logical expression flatness by unwrapping nested OR structures
- The recursion is mandatory for correctness, not just optimization, to properly handle nested structures
- Preserves the original OR clause semantics while creating relation-specific restrictions