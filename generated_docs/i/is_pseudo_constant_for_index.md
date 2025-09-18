# is_pseudo_constant_for_index

## Location
[src/backend/optimizer/path/indxpath.c:3751-3759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3751-L3759)

## Overview
Tests whether a given expression can be used as an indexscan comparison value by checking that it contains no volatile functions and no variables from the index's own table.

## Definition
```c
bool is_pseudo_constant_for_index(PlannerInfo *root, Node *expr, IndexOptInfo *index)
```

## Detailed Description
This function determines if an expression is suitable for use as a comparison value in an index scan. It applies a weaker condition than is_pseudo_constant_clause() by allowing variables from other tables (which enables parameterized index scans). The function performs two key checks: first, it ensures the expression doesn't contain any variables from the index's own table, and second, it verifies the expression contains no volatile functions that could change between evaluations. This is essential for ensuring index scan consistency and correctness.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and metadata
- `expr`: The nodetree expression to be checked for pseudo-constant properties
- `index`: The IndexOptInfo structure representing the index of interest

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md) (bitmap set membership test)
  - [pull_varnos](../p/pull_varnos.md) (extracts relation IDs of all Vars in an expression)
  - [contain_volatile_functions](../c/contain_volatile_functions.md) (checks for volatile function calls in expression tree)
- Called from (representative examples):
  - Referenced in src/include/optimizer/optimizer.h:98 (header declaration)

## Notes and Other Information
- This function is exported for use by planner support functions that have IndexOptInfo available but lack RestrictInfo infrastructure
- The function is optimized by checking variable membership first (via pull_varnos) before the more expensive volatility check
- [Variables](../V/Variables.md) from other tables are permitted, enabling parameterized index scans where the parameter comes from a different relation
- This is a weaker test than is_pseudo_constant_clause(), which would reject any non-constant expressions
- Essential for index scan planning where the comparison value must be stable for the duration of the scan
- Used in conjunction with other index matching functions during query optimization