# IncrementVarSublevelsUp_rtable

## Location
[src/backend/rewrite/rewriteManip.c:873-896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L873-L896)

## Overview
Increments the varlevelsup fields of Var nodes within a range table by a specified delta, used during query rewriting to adjust variable level references when nesting queries.

## Definition

```c
typedef struct
{
	int			rt_index;
	int			sublevels_up;
} rangeTableEntry_used_context;
```
## Detailed Description
This function is a specialized version of IncrementVarSublevelsUp that operates specifically on range tables. It traverses all range table entries and increments the varlevelsup field of Var nodes that meet the minimum sublevels threshold. This is commonly used during query rewriting operations when range tables need to be adjusted for different nesting contexts, such as when pulling up subqueries or restructuring query trees.

The function uses the range_table_walker infrastructure to traverse the range table structure and applies the IncrementVarSublevelsUp_walker function to modify qualifying Var nodes. The QTW_EXAMINE_RTES_BEFORE flag ensures that range table entries are examined before descending into their substructures.

## Parameters
- `rtable`: The range table (List of RangeTblEntry nodes) to process
- `delta_sublevels_up`: The amount to increment varlevelsup fields by
- `min_sublevels_up`: Only increment varlevelsup if it's >= this threshold value

## Dependencies
- Functions called/Symbols referenced:
  - IncrementVarSublevelsUp_context (context structure)
  - range_table_walker (range table traversal function)
  - [IncrementVarSublevelsUp_walker](IncrementVarSublevelsUp_walker.md) (walker function for processing nodes)
  - [QTW_EXAMINE_RTES_BEFORE](../Q/QTW_EXAMINE_RTES_BEFORE.md) (walker flag constant)
- Called from (representative examples):
  - [pull_up_simple_union_all](../p/pull_up_simple_union_all.md) (in query optimization)
  - [ReplaceVarsNoMatchOption](../R/ReplaceVarsNoMatchOption.md) (in rewrite operations)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:873-896
- This is part of PostgreSQL's query rewriting infrastructure
- The function works in conjunction with the walker pattern to efficiently traverse and modify range table structures
- Essential for maintaining correct variable scoping when restructuring queries during optimization or rewriting

## Simplified Source

```c
// Simplified version of IncrementVarSublevelsUp_rtable
void
IncrementVarSublevelsUp_rtable(List *rtable, int delta_sublevels_up,
                              int min_sublevels_up)
{
    IncrementVarSublevelsUp_context context;

    // Setup context for walker
    context.delta_sublevels_up = delta_sublevels_up;
    context.min_sublevels_up = min_sublevels_up;

    // Walk through range table and increment var sublevels
    range_table_walker(rtable,
                      IncrementVarSublevelsUp_walker,
                      (void *) &context,
                      QTW_EXAMINE_RTES_BEFORE);
}
```