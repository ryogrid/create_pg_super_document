# OffsetVarNodes

## Location
[src/backend/rewrite/rewriteManip.c:481-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L481-L532)

## Overview
A public function that adjusts variable node numbers and relation identifiers throughout an expression tree or Query structure by a specified offset.

## Definition

```c
void
OffsetVarNodes(Node *node, int offset, int sublevels_up)
```
## Detailed Description
This function serves as the main entry point for offsetting variable node references in expression trees and Query structures. It handles both Query nodes and bare expression trees, setting up the appropriate context and delegating to OffsetVarNodes_walker for the actual traversal. When starting with a Query node at sublevel 0, it also adjusts Query-specific relation indexes including resultRelation, mergeTargetRelation, exclRelIndex in ON CONFLICT clauses, and relation indexes in rowMarks entries. The function is essential for maintaining correct variable-to-relation mappings when combining range tables or adjusting queries during rewriting and optimization.

## Parameters / Member Variables
- `node`: The root Node (Query or expression) to process
- `offset`: The integer value to add to relation indexes
- `sublevels_up`: The current query nesting level (0 for top level)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - query_tree_walker (for Query node traversal)
  - [OffsetVarNodes_walker](OffsetVarNodes_walker.md) (for actual node processing)
  - OffsetVarNodes_context (context structure)
  - [RowMarkClause](../R/RowMarkClause.md) (for rowMarks processing)
- Called from (representative examples):
  - [convert_EXISTS_sublink_to_join](../c/convert_EXISTS_sublink_to_join.md)
  - [pull_up_simple_subquery](../p/pull_up_simple_subquery.md)
  - [rewriteRuleAction](../r/rewriteRuleAction.md)

## Notes and Other Information
- This is a public function exported in rewriteManip.h
- Handles both Query nodes and bare expression trees as starting points
- For Query nodes at top level (sublevels_up == 0), adjusts Query-specific indexes:
  - resultRelation (target table for INSERT/UPDATE/DELETE)
  - mergeTargetRelation (target for MERGE statements)
  - exclRelIndex (excluded table index for ON CONFLICT)
  - rti values in rowMarks entries
- The sublevels_up parameter allows correct handling of nested subqueries
- Used extensively during query rewriting, subquery pullup, and rule application
- Critical for maintaining referential integrity when combining or transforming queries
- Works in conjunction with CombineRangeTables to handle complete query merging operations

## Simplified Source

```c
void OffsetVarNodes(Node *node, int offset, int sublevels_up) {
    // Set up context for the walker function
    OffsetVarNodes_context context;
    context.offset = offset;
    context.sublevels_up = sublevels_up;

    // Handle Query nodes differently from expression trees
    if (node && IsA(node, Query)) {
        Query *qry = (Query *) node;

        // For top-level queries, also adjust Query-specific relation indexes
        if (sublevels_up == 0) {
            // Adjust result relation indexes
            if (qry->resultRelation)
                qry->resultRelation += offset;

            if (qry->mergeTargetRelation)
                qry->mergeTargetRelation += offset;

            // Adjust ON CONFLICT excluded relation index
            if (qry->onConflict && qry->onConflict->exclRelIndex)
                qry->onConflict->exclRelIndex += offset;

            // Adjust row mark relation indexes
            ListCell *l;
            foreach(l, qry->rowMarks) {
                RowMarkClause *rc = (RowMarkClause *) lfirst(l);
                rc->rti += offset;
            }
        }

        // Use query tree walker for proper Query traversal
        query_tree_walker(qry, OffsetVarNodes_walker, (void *) &context, 0);
    } else {
        // For bare expression trees, call walker directly
        OffsetVarNodes_walker(node, &context);
    }
}
```