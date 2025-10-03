# relation_excluded_by_constraints

## Location
[src/backend/optimizer/util/plancat.c:1576-1763](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1576-L1763)

## Overview
Determines whether a relation can be excluded from scanning based on constraint analysis, detecting self-inconsistent restrictions or restrictions that contradict the relation's constraints.

## Definition

```c
bool
relation_excluded_by_constraints(PlannerInfo *root,
								 RelOptInfo *rel, RangeTblEntry *rte)
```
## Detailed Description
This function performs constraint exclusion optimization by analyzing whether a relation needs to be scanned at all. It examines the relation's base restriction clauses and compares them against the relation's constraints (CHECK constraints, NOT NULL constraints, and partition constraints) to determine if the restrictions are logically inconsistent with the constraints, making the scan unnecessary.

The function operates in multiple phases:
1. **Constant FALSE detection**: Identifies restriction clauses that are constant FALSE or NULL
2. **Constraint exclusion mode checking**: Respects the  GUC setting (off/partition/on)
3. **Self-contradiction analysis**: Checks if restriction clauses contradict each other
4. **Constraint contradiction analysis**: Verifies if restrictions are refuted by relation constraints

The function only works with simple relations and requires immutable functions to make safe deductions during planning.

## Parameters / Member Variables
- `*root`: PlannerInfo containing global planner state and configuration
- `*rel`: RelOptInfo representing the relation being analyzed (must be a simple relation)
- `*rte`: RangeTblEntry containing metadata about the relation from the range table
## Dependencies
- Functions called/Symbols referenced:
  - IS_SIMPLE_REL (macro for checking simple relation type)
  - [contain_mutable_functions](../c/contain_mutable_functions.md) (checks for mutable functions in expressions)
  - [predicate_refuted_by](../p/predicate_refuted_by.md) (logical refutation testing)
  - [get_relation_constraints](../g/get_relation_constraints.md) (retrieves relation's constraint expressions)
  - CONSTRAINT_EXCLUSION_OFF/PARTITION/ON (GUC setting values)
  - RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL (relation optimization kinds)
  - RTE_RELATION (range table entry kind)

- Called from (representative examples):
  - [set_rel_size](../s/set_rel_size.md) (src/backend/optimizer/path/allpaths.c:364)
  - [set_append_rel_size](../s/set_append_rel_size.md) (src/backend/optimizer/path/allpaths.c:1027)

## Notes and Other Information
- Only processes simple relations (Assert(IS_SIMPLE_REL(rel)) enforced)
- Respects the constraint_exclusion GUC setting to control optimization behavior
- Uses weak refutation for self-contradictory restrictions, strong refutation for constraint contradictions
- Handles inheritance scenarios carefully, considering NO INHERIT and NOT NULL constraint inheritance
- Critical for partition pruning and constraint-based optimization in PostgreSQL query planning
- Location: src/backend/optimizer/util/plancat.c:1576-1763

## Simplified Source

```c
bool relation_excluded_by_constraints(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte) {
    bool include_noinherit, include_notnull, include_partition = false;
    List *safe_restrictions;
    List *constraint_pred;
    List *safe_constraints;
    ListCell *lc;

    // Must be a simple relation
    Assert(IS_SIMPLE_REL(rel));

    // No restrictions means no exclusion possible
    if (rel->baserestrictinfo == NIL)
        return false;

    // Phase 1: Check for constant FALSE/NULL restrictions
    foreach(lc, rel->baserestrictinfo) {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
        Expr *clause = rinfo->clause;

        if (clause && IsA(clause, Const) &&
            (((Const *) clause)->constisnull ||
             !DatumGetBool(((Const *) clause)->constvalue))) {
            return true;  // Found FALSE constraint - exclude relation
        }
    }

    // Phase 2: Check constraint_exclusion GUC setting
    switch (constraint_exclusion) {
        case CONSTRAINT_EXCLUSION_OFF:
            return false;  // No further checks

        case CONSTRAINT_EXCLUSION_PARTITION:
            // Only process appendrel members
            if (rel->reloptkind != RELOPT_OTHER_MEMBER_REL)
                return false;
            break;

        case CONSTRAINT_EXCLUSION_ON:
            // Include partition constraints for base relations
            if (rel->reloptkind == RELOPT_BASEREL)
                include_partition = true;
            break;
    }

    // Phase 3: Check for self-contradictory restrictions
    safe_restrictions = NIL;
    foreach(lc, rel->baserestrictinfo) {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        if (!contain_mutable_functions((Node *) rinfo->clause))
            safe_restrictions = lappend(safe_restrictions, rinfo->clause);
    }

    if (predicate_refuted_by(safe_restrictions, safe_restrictions, true))
        return true;  // Self-contradictory restrictions

    // Only plain relations have constraints
    if (rte->rtekind != RTE_RELATION)
        return false;

    // Phase 4: Get relation constraints and test for contradiction
    include_noinherit = !rte->inh;
    include_notnull = (!rte->inh || rte->relkind == RELKIND_PARTITIONED_TABLE);

    constraint_pred = get_relation_constraints(root, rte->relid, rel,
                                             include_noinherit, include_notnull,
                                             include_partition);

    // Filter out mutable constraints
    safe_constraints = NIL;
    foreach(lc, constraint_pred) {
        Node *pred = (Node *) lfirst(lc);

        if (!contain_mutable_functions(pred))
            safe_constraints = lappend(safe_constraints, pred);
    }

    // Test if constraints refute the restrictions
    if (predicate_refuted_by(safe_constraints, rel->baserestrictinfo, false))
        return true;  // Constraints contradict restrictions

    return false;  // Relation cannot be excluded
}
```