# match_funcclause_to_indexcol

## Location
[src/backend/optimizer/path/indxpath.c:2511-2556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L2511-L2556)

## Overview
Handles FuncExpr (function expression) cases for index clause matching by leveraging planner support functions to determine indexability.

## Definition
```c
static IndexClause *
match_funcclause_to_indexcol(PlannerInfo *root,
                             RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index)
```

## Detailed Description
This function processes function expressions for potential index usage by delegating to planner support functions. Unlike operator-based matching, function clause matching requires specialized knowledge that is provided by the function's attached planner support function.

The function employs a performance optimization strategy:
1. **Argument scanning**: Iterates through all function arguments to find matches with the target index column
2. **Selective invocation**: Only calls the planner support function when at least one argument matches the index column, avoiding wasteful planning cycles
3. **Support function delegation**: Relies on `get_index_clause_from_support` to handle the complex logic of determining indexability

The approach is deliberately liberal regarding non-matching arguments - the support function is responsible for validating whether other arguments are appropriate pseudoconstants. This flexibility enables scenarios where only some arguments need to be included in the indexqual.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and statistics
- `rinfo`: RestrictInfo node wrapping the FuncExpr to be analyzed for index compatibility  
- `indexcol`: Zero-based column number of the target index column
- `index`: IndexOptInfo structure containing metadata about the candidate index

## Dependencies
- Functions called/Symbols referenced:
  - [match_index_to_operand](match_index_to_operand.md)
  - [get_index_clause_from_support](../g/get_index_clause_from_support.md)
  - lfirst (list traversal)
- Called from (representative examples):
  - ec_member_matches_arg
  - [match_clause_to_indexcol](match_clause_to_indexcol.md)

## Notes and Other Information
- No built-in intelligence for function clauses - entirely depends on planner support functions
- Performance-optimized to avoid unnecessary support function calls when no arguments match
- Supports partial argument matching where only some function arguments are indexable
- The support function validates pseudoconstant requirements for non-indexed arguments
- Part of PostgreSQL's extensible indexing framework allowing custom index access methods
- Enables advanced indexing strategies for specialized functions and operators
- Located in `src/backend/optimizer/path/indxpath.c:2511-2556`
- Returns IndexClause via support function or NULL if no indexable pattern is found