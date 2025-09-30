# build_simple_rel

## Location
[src/backend/optimizer/util/relnode.c:192-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L192-L413)

## Overview
Constructs a new RelOptInfo structure for a base relation or other member relation, initializing all necessary fields and handling inheritance hierarchies.

## Definition
```c
RelOptInfo *build_simple_rel(PlannerInfo *root, int relid, RelOptInfo *parent)
```

## Detailed Description
This function creates and initializes a RelOptInfo structure, which is the fundamental data structure used by the PostgreSQL query planner to represent relations. The function handles different types of relations (tables, subqueries, functions, CTEs, etc.) and properly sets up inheritance relationships when a parent is provided.

Key responsibilities include:
- Creating a new RelOptInfo node with appropriate default values
- Setting up relation type (base relation or other member relation)
- Configuring user permissions and access control
- Handling inheritance hierarchy propagation (lateral references, nulling relations)
- Initializing attribute ranges and arrays based on relation type
- Applying parent quals to child relations through apply_child_basequals
- Storing the completed RelOptInfo in the simple_rel_array

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state
- `relid`: Range table index (1-based) identifying the relation 
- `parent`: Parent RelOptInfo for inheritance hierarchies, NULL for base relations

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create RelOptInfo structure)
  - [bms_make_singleton](bms_make_singleton.md) (for creating relation ID bitmaps)
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md) (for target list initialization)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md) (for permission handling)
  - [get_relation_info](../g/get_relation_info.md) (for table statistics and metadata)
  - [apply_child_basequals](../a/apply_child_basequals.md) (for constraint inheritance)
  - [mark_dummy_rel](../m/mark_dummy_rel.md) (when constraints prove relation is empty)
- Constants used:
  - RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL (relation optimization kinds)
  - RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION, etc. (range table entry types)
- Data structures used:
  - [RelOptInfo](../R/RelOptInfo.md), RangeTblEntry, RTEPermissionInfo, AppendRelInfo
- Called from (representative examples):
  - [add_base_rels_to_query](../a/add_base_rels_to_query.md) (src/backend/optimizer/plan/initsplan.c:165)
  - [query_planner](../q/query_planner.md) (src/backend/optimizer/plan/planmain.c:107)
  - [expand_inherited_rtentry](../e/expand_inherited_rtentry.md) (src/backend/optimizer/util/inherit.c:221)
  - [expand_partitioned_rtentry](../e/expand_partitioned_rtentry.md) (src/backend/optimizer/util/inherit.c:414)

## Notes and Other Information
- Includes comprehensive validation to prevent duplicate relation creation
- Handles complex inheritance scenarios including lateral references and nulling relations
- Different initialization paths for different RTE types (relations vs subqueries vs functions)
- The function can mark child relations as dummy if inherited constraints prove they will be empty
- Must store RelOptInfo in simple_rel_array before calling apply_child_basequals due to transformation dependencies
- Supports both regular inheritance and partitioned table hierarchies

## Simplified Source

```c
RelOptInfo *build_simple_rel(PlannerInfo *root, int relid, RelOptInfo *parent)
{
    RelOptInfo *rel;
    RangeTblEntry *rte;

    // Validate that relation doesn't already exist
    Assert(relid > 0 && relid < root->simple_rel_array_size);
    if (root->simple_rel_array[relid] != NULL)
        elog(ERROR, "rel %d already exists", relid);

    // Get the range table entry for this relation
    rte = root->simple_rte_array[relid];
    Assert(rte != NULL);

    // Create and initialize new RelOptInfo structure
    rel = makeNode(RelOptInfo);
    rel->reloptkind = parent ? RELOPT_OTHER_MEMBER_REL : RELOPT_BASEREL;
    rel->relids = bms_make_singleton(relid);
    rel->relid = relid;
    rel->rtekind = rte->rtekind;

    // Initialize optimization-related fields
    rel->rows = 0;
    rel->consider_startup = (root->tuple_fraction > 0);
    rel->reltarget = create_empty_pathtarget();
    rel->pathlist = NIL;
    rel->cheapest_startup_path = NULL;
    rel->cheapest_total_path = NULL;

    // Set up user permissions for access control
    if (rte->rtekind == RTE_RELATION) {
        if (rel->reloptkind == RELOPT_BASEREL ||
            (rel->reloptkind == RELOPT_OTHER_MEMBER_REL &&
             parent->rtekind == RTE_SUBQUERY)) {
            RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);
            rel->userid = perminfo->checkAsUser;
        } else {
            rel->userid = parent->userid;
        }
    }

    // Handle inheritance hierarchy propagation
    if (parent) {
        rel->parent = parent;
        rel->top_parent = parent->top_parent ? parent->top_parent : parent;
        rel->top_parent_relids = rel->top_parent->relids;
        rel->nulling_relids = parent->nulling_relids;

        // Propagate lateral reference information
        rel->direct_lateral_relids = parent->direct_lateral_relids;
        rel->lateral_relids = parent->lateral_relids;
        rel->lateral_referencers = parent->lateral_referencers;
    }

    // Set up attributes based on relation type
    switch (rte->rtekind) {
        case RTE_RELATION:
            // Table - get statistics from system catalogs
            get_relation_info(root, rte->relid, rte->inh, rel);
            break;

        case RTE_SUBQUERY:
        case RTE_FUNCTION:
        case RTE_TABLEFUNC:
        case RTE_VALUES:
        case RTE_CTE:
        case RTE_NAMEDTUPLESTORE:
            // Set up attribute range and arrays for non-table relations
            rel->min_attr = 0;
            rel->max_attr = list_length(rte->eref->colnames);
            rel->attr_needed = (Relids *) palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(Relids));
            rel->attr_widths = (int32 *) palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(int32));
            break;

        case RTE_RESULT:
            // Result relations have no columns
            rel->min_attr = 0;
            rel->max_attr = -1;
            rel->attr_needed = NULL;
            rel->attr_widths = NULL;
            break;
    }

    // Store the relation in the simple_rel_array
    root->simple_rel_array[relid] = rel;

    // Apply parent quals to child relations
    if (parent) {
        AppendRelInfo *appinfo = root->append_rel_array[relid];
        if (!apply_child_basequals(root, parent, rel, rte, appinfo)) {
            // If quals reduce to FALSE, mark relation as dummy
            mark_dummy_rel(rel);
        }
    }

    return rel;
}
```