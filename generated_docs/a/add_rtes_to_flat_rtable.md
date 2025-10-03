# add_rtes_to_flat_rtable

## Location
[src/backend/optimizer/plan/setrefs.c:391-479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L391-L479)

## Overview
Extracts RangeTblEntries from the plan's rangetable and adds them to the flat rangetable, handling both live and dead subqueries.

## Definition

```c
static void
add_rtes_to_flat_rtable(PlannerInfo *root, bool recursing)
```
## Detailed Description
The  function is responsible for consolidating range table entries from various query levels into a single flattened rangetable. It operates in two distinct phases:

**Phase 1 - Live RTEs**: Processes the query's own RTEs and adds them to the flattened rangetable. At the top level, all RTEs are added to maintain index consistency. When recursing into subqueries, only relation RTEs and subquery RTEs that were once relation RTEs (identified by valid relid) are processed.

**Phase 2 - Dead Subqueries**: Handles subqueries that are not referenced in the Plan tree but still need their RTEs added to ensure proper permission checks during execution. This includes:
- Unplanned subqueries excluded due to self-contradictory constraints
- Dummy subqueries omitted from the plan tree
- Recursively processing subquery RTEs when appropriate

The function intelligently determines whether to flatten unplanned RTEs or recursively process subquery root depending on the planning state and whether the subquery result relation is dummy.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing the current query's planning context and rangetable
- `recursing`: Boolean flag indicating whether this is a recursive call into a subquery (affects which RTEs are processed)
## Dependencies
- Functions called/Symbols referenced:
  - [add_rte_to_flat_rtable](add_rte_to_flat_rtable.md)
  - [flatten_unplanned_rtes](../f/flatten_unplanned_rtes.md)
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - IS_DUMMY_REL
  - [add_rtes_to_flat_rtable](add_rtes_to_flat_rtable.md) (recursive call)
- Constants used:
  - RTE_RELATION
  - RTE_SUBQUERY
  - UPPERREL_FINAL
- Types used:
  - [PlannerGlobal](../P/PlannerGlobal.md)
- Called from (representative examples):
  - [set_plan_references](../s/set_plan_references.md)
  - fix_scan_list
  - [add_rtes_to_flat_rtable](add_rtes_to_flat_rtable.md) (recursive)

## Notes and Other Information
- Processes RTEs in two separate passes to maintain proper numbering in the flattened rangetable
- Handles inheritance-parent RTEs by ignoring them since their contents are already pulled up
- Ensures permission checks are performed for all tables, even those in dead subqueries
- Uses RelOptInfo array to determine subquery planning state and decide on processing approach
- Recursively calls itself when processing nested subqueries

## Simplified Source

```c
static void add_rtes_to_flat_rtable(PlannerInfo *root, bool recursing) {
    PlannerGlobal *glob = root->glob;
    Index rti;
    ListCell *lc;

    // Phase 1: Add live RTEs to flattened rangetable
    foreach(lc, root->parse->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

        // At top level: add all RTEs; when recursing: only relations and converted subqueries
        if (!recursing || rte->rtekind == RTE_RELATION ||
            (rte->rtekind == RTE_SUBQUERY && OidIsValid(rte->relid))) {
            add_rte_to_flat_rtable(glob, root->parse->rteperminfos, rte);
        }
    }

    // Phase 2: Handle dead subqueries for permission checks
    rti = 1;
    foreach(lc, root->parse->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

        // Process non-inheritance subquery RTEs that have RelOptInfo entries
        if (rte->rtekind == RTE_SUBQUERY && !rte->inh &&
            rti < root->simple_rel_array_size) {

            RelOptInfo *rel = root->simple_rel_array[rti];
            if (rel != NULL) {
                Assert(rel->relid == rti);

                // Handle unplanned or dummy subqueries
                if (rel->subroot == NULL) {
                    // Unplanned subquery - flatten its RTEs
                    flatten_unplanned_rtes(glob, rte);
                }
                else if (recursing ||
                         IS_DUMMY_REL(fetch_upper_rel(rel->subroot, UPPERREL_FINAL, NULL))) {
                    // Recursively process subquery's rangetable
                    add_rtes_to_flat_rtable(rel->subroot, true);
                }
            }
        }
        rti++;
    }
}
```