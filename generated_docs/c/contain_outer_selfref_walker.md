# contain_outer_selfref_walker

## Location
[src/backend/optimizer/plan/subselect.c:1097-1137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1097-L1137)

## Overview
A recursive tree walker function that traverses query nodes to detect external recursive self-references in CTEs (Common Table Expressions) by examining range table entries and tracking query nesting depth.

## Definition

```c
struct inline_cte_walker_context context;
```
## Detailed Description
This function performs the actual tree traversal work for detecting external recursive self-references in PostgreSQL query trees. It implements a depth-first search through the query tree structure, maintaining a depth counter to track the current query nesting level. The core logic identifies problematic CTE self-references where a CTE references itself from a query level that is equal to or higher than the CTE's definition level (indicated by ).

The function handles three main node types:
1. **RangeTblEntry nodes**: Checks for CTE self-references with inappropriate nesting levels
2. **Query nodes**: Recursively processes subqueries while properly managing depth tracking
3. **Other expression nodes**: Delegates to standard expression tree walking

The depth tracking is critical because it allows the function to distinguish between legitimate recursive references (within the same query level) and problematic external references (crossing query boundaries).

## Parameters / Member Variables
- : The current Node being examined in the tree traversal
- : Pointer to an Index tracking the current query nesting depth (modified during traversal)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - query_tree_walker
  - expression_tree_walker
  - [QTW_EXAMINE_RTES_BEFORE](../Q/QTW_EXAMINE_RTES_BEFORE.md) (flag constant)
  - RTE_CTE (enum value)
- Called from (representative examples):
  - [contain_outer_selfref](contain_outer_selfref.md)
  - [contain_outer_selfref_walker](contain_outer_selfref_walker.md) (recursive calls)

## Notes and Other Information
- This is a recursive function that calls itself when processing Query nodes and expression nodes
- The depth parameter is incremented when entering a subquery and decremented when exiting
- Uses the QTW_EXAMINE_RTES_BEFORE flag to ensure range table entries are examined before other query components
- Returns true immediately upon finding the first external self-reference, short-circuiting further traversal
- The function distinguishes between self-references at the same level (allowed) vs. external levels (problematic)
- Static function scope limits visibility to the subselect.c compilation unit

## Simplified Source

```c
static bool
contain_outer_selfref_walker(Node *node, Index *depth)
{
    if (node == NULL)
        return false;

    // Check range table entries for problematic CTE self-references
    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry *rte = (RangeTblEntry *) node;

        // Found external CTE self-reference (above current query level)
        if (rte->rtekind == RTE_CTE &&
            rte->self_reference &&
            rte->ctelevelsup >= *depth)
            return true;

        return false;
    }

    // Recursively process subqueries with proper depth tracking
    if (IsA(node, Query)) {
        Query *query = (Query *) node;
        bool result;

        (*depth)++;
        result = query_tree_walker(query, contain_outer_selfref_walker,
                                 (void *) depth, QTW_EXAMINE_RTES_BEFORE);
        (*depth)--;

        return result;
    }

    // Process other expression nodes
    return expression_tree_walker(node, contain_outer_selfref_walker,
                                (void *) depth);
}
```