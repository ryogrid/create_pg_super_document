# flatten_grouping_sets

## Location
[src/backend/parser/parse_clause.c:2258-2366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L2258-L2366)

## Overview
Flattens out parenthesized sublists in grouping lists and handles nested grouping sets according to SQL specification requirements, while preserving CUBE and ROLLUP syntax for deparsing.

## Definition

```c
static Node *
flatten_grouping_sets(Node *expr, bool toplevel, bool *hasGroupingSets)
```
## Detailed Description
This function performs syntax transformations on grouping set expressions to normalize their structure while maintaining compliance with SQL specifications. It handles several key transformations:

1. **Nested GROUPING SETS flattening**: Converts nested GROUPING SETS into a single level
   -  becomes 

2. **RowExpr handling**: Processes implicit cast row expressions by recursively flattening their arguments

3. **List processing**: Recursively processes lists of expressions, concatenating nested lists and preserving non-empty results

The function preserves CUBE and ROLLUP syntax within GROUPING SETS to maintain the originally specified grouping set syntax for deparsing, while full expansion is left to the planner. It also handles pathological input by checking stack depth to prevent infinite recursion.

## Parameters / Member Variables
- : The grouping expression node to be flattened (can be a single expression, GroupingSet, RowExpr, or List)
- : Boolean flag indicating whether this is a top-level call (affects how empty grouping sets and nested sets are handled)
- : Output parameter (can be NULL) that gets set to true if any GroupingSet nodes are encountered during processing

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - [flatten_grouping_sets](flatten_grouping_sets.md) (recursive calls)
  - [makeGroupingSet](../m/makeGroupingSet.md)
  - [list_concat](../l/list_concat.md)
  - lappend
  - lfirst
  - RowExpr
  - GroupingSet
  - COERCE_IMPLICIT_CAST
  - GROUPING_SET_EMPTY
  - GROUPING_SET_SETS
- Called from (representative examples):
  - [transformGroupClause](../t/transformGroupClause.md)
  - [flatten_grouping_sets](flatten_grouping_sets.md) (recursive calls)

## Notes and Other Information
- This is a static recursive function within parse_clause.c for internal parser use
- Implements SQL specification syntax transformations for grouping sets
- Preserves original CUBE and ROLLUP syntax to maintain query readability in deparsing
- Handles nested grouping sets up to 2 levels deep as per SQL specification
- Uses stack depth checking to prevent stack overflow on pathological input
- At the top level, empty grouping sets are skipped (caller can supply canonical GROUP BY () if needed)
- The function creates new lists but doesn't deep-copy old nodes except for GroupingSet nodes
- Sets the hasGroupingSets flag as a side effect when GroupingSet nodes are encountered