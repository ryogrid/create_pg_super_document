# contain_exec_param_walker

## Location
src/backend/optimizer/util/clauses.c: 1143 - 1178

## Overview
A recursive tree walker function that traverses expression nodes to locate PARAM_EXEC parameters with specific parameter IDs.

## Definition
```c
static bool contain_exec_param_walker(Node *node, List *param_ids)
```

## Detailed Description
This function implements a specialized tree walker that recursively examines PostgreSQL expression trees to find PARAM_EXEC parameters whose parameter IDs match those specified in the provided list. When encountering a Param node, it checks if the parameter kind is PARAM_EXEC and if the parameter ID exists in the target list. The function uses PostgreSQL's expression_tree_walker infrastructure to ensure comprehensive traversal of the expression tree. This walker is essential for dependency analysis in query planning, helping to identify which execution parameters are referenced by specific expressions.

## Parameters / Member Variables
- `node`: The expression node to examine for PARAM_EXEC parameters
- `param_ids`: A list of integer parameter IDs to match against when encountering PARAM_EXEC parameters

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - list_member_int
  - expression_tree_walker
  - contain_exec_param_walker (recursive calls)
- Data types referenced:
  - Param
  - PARAM_EXEC
- Called from (representative examples):
  - contain_exec_param
  - contain_exec_param_walker (recursive calls)

## Notes and Other Information
- Specifically designed for PARAM_EXEC parameter detection, ignoring other parameter types
- Uses list_member_int for efficient parameter ID matching
- Returns true immediately upon finding the first matching PARAM_EXEC parameter
- Part of the parameter dependency analysis system in PostgreSQL's optimizer
- Located in src/backend/optimizer/util/clauses.c at lines 1143-1178
- The recursive nature ensures all nested expressions are examined for parameter references