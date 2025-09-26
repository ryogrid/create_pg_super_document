# PlaceHolderInfo

## Location
[src/include/nodes/pathnodes.h:3074-3100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L3074-L3100)

## Overview
PlaceHolderInfo is a centralized data structure that stores metadata for placeholder expressions during query planning, managing where placeholders should be evaluated and where their values are needed in the join tree.

## Definition
```c
typedef struct PlaceHolderInfo
{
    pg_node_attr(no_read, no_query_jumble)

    NodeTag     type;

    /* ID for PH (unique within planner run) */
    Index       phid;

    /*
     * copy of PlaceHolderVar tree (should be redundant for comparison, could
     * be ignored)
     */
    PlaceHolderVar *ph_var;

    /* lowest level we can evaluate value at */
    Relids      ph_eval_at;

    /* relids of contained lateral refs, if any */
    Relids      ph_lateral;

    /* highest level the value is needed at */
    Relids      ph_needed;

    /* estimated attribute width */
    int32       ph_width;
} PlaceHolderInfo;
```

## Detailed Description
PlaceHolderInfo serves as the central coordination point for placeholder expressions in PostgreSQL's query planner. For each distinct placeholder expression generated during planning, one PlaceHolderInfo node is stored in the PlannerInfo's placeholder_list. This design centralizes information that would otherwise need to be duplicated across multiple PlaceHolderVar copies.

The core concept is to evaluate the placeholder expression at exactly the ph_eval_at join level, then allow the result to bubble up through the join tree like a regular Var until it reaches the ph_needed level. This mechanism enables proper handling of complex expressions that need to be computed at specific points in the join order.

The structure also handles LATERAL references, where placeholder expressions might contain references to variables from outside their syntactic scope. These external relations are recorded in ph_lateral but are not included in ph_eval_at.

Importantly, PlaceHolderInfo can create join order constraints - the ph_eval_at join must be formed below any outer joins that should null the PlaceHolderVar, ensuring correct NULL-handling semantics.

## Parameters / Member Variables
- : Standard NodeTag for node type identification
- : Unique identifier for this placeholder, unique across the entire planner run (not just within a query level)
- : Copy of the PlaceHolderVar tree structure (kept for comparison purposes, could potentially be ignored)
- : Bitmap of relation IDs representing the lowest join level where this placeholder's value can be computed
- : Bitmap of relation IDs for any LATERAL references contained within the placeholder expression
- : Bitmap of relation IDs representing the highest join level where this placeholder's value is required (similar semantics to attr_needed for regular Vars)
- : Estimated width in bytes of the placeholder's computed value

## Dependencies
- Functions called/Symbols referenced:
  - PlaceHolderVar (placeholder variable node)
  - NodeTag (node type system)
  - Relids (relation ID bitmap)
  - Index (unique identifier type)

- Called from (representative examples):
  - make_placeholder_expr (in placeholder.c:82)
  - find_placeholder_info (in placeholder.c:85, 106, 159, 162)
  - add_placeholders_to_base_rels (in placeholder.c:335)
  - add_placeholders_to_joinrel (in placeholder.c:383)
  - have_join_order_restriction (in joinrels.c:1095)
  - join_is_removable (in analyzejoins.c:234)

## Notes and Other Information
- Uses pg_node_attr with no_read and no_query_jumble attributes to control node processing behavior
- Only created after determining that the PlaceHolderVar is actually referenced in the plan tree, avoiding unnecessary join order constraints for unreferenced placeholders
- The phid uniqueness across planner runs eliminates the need to reassign IDs when pulling subqueries into parent queries
- Critical for maintaining proper evaluation semantics in complex queries with subqueries, LATERAL joins, and outer joins
- Part of PostgreSQL's sophisticated expression evaluation and join ordering optimization system