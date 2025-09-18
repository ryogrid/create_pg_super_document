# match_opclause_to_indexcol

## Location
src/backend/optimizer/path/indxpath.c: 2392 - 2510

## Overview
Handles OpExpr (operator expression) cases for index clause matching, determining if binary operator clauses can be used with a specific index column.

## Definition
```c
static IndexClause *
match_opclause_to_indexcol(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index)
```

## Detailed Description
This function processes binary operator expressions to determine their compatibility with index columns. It handles two primary patterns:

1. **Left index pattern**: `(indexkey operator constant)` - Direct index usage
2. **Right index pattern**: `(constant operator indexkey)` - Requires operator commutation

The function performs comprehensive validation including:

- **Binary operator verification**: Only processes expressions with exactly two operands
- **Index key matching**: Uses `match_index_to_operand` to verify operand corresponds to index column
- **Volatility checking**: Ensures non-index operands don't contain volatile functions using `contain_volatile_functions`
- **Relation membership**: Confirms non-index operands don't reference the indexed relation
- **Operator family membership**: Validates operators belong to index's operator family via `op_in_opfamily`
- **Collation compatibility**: Matches expression and index collations using `IndexCollMatchesExprColl`

For right-index patterns, the function attempts operator commutation using `get_commutator` and `commute_restrictinfo` to transform the clause into executable form.

When standard operator matching fails, the function falls back to planner support functions via `get_index_clause_from_support` for advanced indexing strategies.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and cost information
- `rinfo`: RestrictInfo node wrapping the OpExpr clause to be tested
- `indexcol`: Zero-based column number within the target index
- `index`: IndexOptInfo structure containing index metadata and operator families

## Dependencies
- Functions called/Symbols referenced:
  - match_index_to_operand
  - bms_is_member
  - contain_volatile_functions
  - IndexCollMatchesExprColl
  - op_in_opfamily
  - get_commutator
  - commute_restrictinfo
  - set_opfuncid
  - get_index_clause_from_support
  - linitial/lsecond (list access)
  - makeNode (IndexClause creation)
- Called from (representative examples):
  - ec_member_matches_arg
  - match_clause_to_indexcol

## Notes and Other Information
- Only processes binary operators (expressions with exactly 2 arguments)
- Supports operator commutation to handle `constant op indexkey` patterns
- Falls back to planner support functions for complex indexing scenarios
- Performs strict validation on volatility, relation membership, and collation compatibility
- Part of the comprehensive index optimization system in PostgreSQL's query planner
- Located in `src/backend/optimizer/path/indxpath.c:2392-2510`
- Returns non-lossy IndexClause nodes for standard operator matching cases