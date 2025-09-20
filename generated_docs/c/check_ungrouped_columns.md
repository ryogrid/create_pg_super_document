# check_ungrouped_columns

## Location
[src/backend/parser/parse_agg.c:1275-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1275-L1294)

## Overview
Scans an expression tree to detect ungrouped variables (variables not in GROUP BY clauses and not within aggregate functions) and emits appropriate error messages.

## Definition

```c
static void
check_ungrouped_columns(Node *node, ParseState *pstate, Query *qry,
						List *groupClauses, List *groupClauseCommonVars,
						bool have_non_var_grouping,
						List **func_grouped_rels)
```
## Detailed Description
This function serves as the entry point for ungrouped column validation by:

1. **Context setup**: Initializes a check_ungrouped_columns_context structure with all necessary information for the validation process
2. **Walker invocation**: Delegates the actual tree traversal and validation to check_ungrouped_columns_walker, which uses the expression_tree_walker framework
3. **Assumption management**: Assumes that the caller has already flattened join variables (hasJoinRTEs = false) and that the expression tree has been properly transformed for parser output

The function implements SQL's GROUP BY semantics by ensuring that any variable referenced outside of aggregate functions must either be:
- Listed in the GROUP BY clause
- Functionally dependent on grouped columns
- Within aggregate function arguments

## Parameters / Member Variables
- : Expression tree to scan for ungrouped variables
- : Parser state providing context and error reporting capabilities
- : Query structure containing grouping information
- : List of acceptable GROUP BY expressions (TargetEntry nodes)
- : Variables present in all grouping sets (for functional dependency checking)
- : Whether grouping expressions include non-variable expressions
- : Output parameter for relations that are functionally grouped

## Dependencies
- Functions called/Symbols referenced:
  - [check_ungrouped_columns_walker](check_ungrouped_columns_walker.md)
  - check_ungrouped_columns_context (struct)
- Called from (representative examples):
  - [parseCheckAggregates](../p/parseCheckAggregates.md) (twice - for target list and HAVING clause)

## Notes and Other Information
- Uses expression_tree_walker for efficient tree traversal
- Recognizes grouping expressions in main queries but only grouping Vars in subqueries due to sublevel complexity
- The limitation on subquery grouping expressions exists because implementing full equal() comparison across different sublevels_up would be complex
- Assumes join variables have been flattened by the caller for proper equality comparison
- Part of PostgreSQL's comprehensive GROUP BY validation system