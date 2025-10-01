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
  - [check_stack_depth](../c/check_stack_depth.md)
  - [flatten_grouping_sets](flatten_grouping_sets.md) (recursive calls)
  - [makeGroupingSet](../m/makeGroupingSet.md)
  - [list_concat](../l/list_concat.md)
  - [lappend](../l/lappend.md)
  - lfirst
  - RowExpr
  - [GroupingSet](../G/GroupingSet.md)
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

## Simplified Source

```c
static Node *flatten_grouping_sets(Node *expr, bool toplevel, bool *hasGroupingSets) {
    // Prevent stack overflow on pathological input
    check_stack_depth();

    if (expr == (Node *) NIL)
        return (Node *) NIL;

    switch (expr->type) {
        case T_RowExpr: {
            RowExpr *r = (RowExpr *) expr;
            // Handle implicit cast row expressions
            if (r->row_format == COERCE_IMPLICIT_CAST)
                return flatten_grouping_sets((Node *) r->args, false, NULL);
            break;
        }

        case T_GroupingSet: {
            GroupingSet *gset = (GroupingSet *) expr;
            List *result_set = NIL;

            if (hasGroupingSets)
                *hasGroupingSets = true;

            // Skip empty grouping sets at top level
            if (toplevel && gset->kind == GROUPING_SET_EMPTY)
                return (Node *) NIL;

            // Process each element in the grouping set
            foreach(l2, gset->content) {
                Node *n1 = lfirst(l2);
                Node *n2 = flatten_grouping_sets(n1, false, NULL);

                // Flatten nested GROUPING SETS
                if (IsA(n1, GroupingSet) && ((GroupingSet *) n1)->kind == GROUPING_SET_SETS)
                    result_set = list_concat(result_set, (List *) n2);
                else
                    result_set = lappend(result_set, n2);
            }

            // Handle top level vs nested grouping sets
            if (toplevel || (gset->kind != GROUPING_SET_SETS))
                return (Node *) makeGroupingSet(gset->kind, result_set, gset->location);
            else
                return (Node *) result_set;
        }

        case T_List: {
            List *result = NIL;

            // Recursively process list elements
            foreach(l, (List *) expr) {
                Node *n = flatten_grouping_sets(lfirst(l), toplevel, hasGroupingSets);

                if (n != (Node *) NIL) {
                    if (IsA(n, List))
                        result = list_concat(result, (List *) n);
                    else
                        result = lappend(result, n);
                }
            }
            return (Node *) result;
        }

        default:
            break;
    }

    return expr;  // Return unchanged for other node types
}
```