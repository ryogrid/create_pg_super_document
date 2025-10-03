# fix_indexqual_references

## Location
[src/backend/optimizer/plan/createplan.c:5023-5063](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5023-L5063)

## Overview
Adjusts indexqual clauses to the form required by PostgreSQL's executor's indexqual machinery, handling qual extraction, parameter replacement, and index key mapping.

## Definition

```c
static void
fix_indexqual_references(PlannerInfo *root, IndexPath *index_path,
						 List **stripped_indexquals_p, List **fixed_indexquals_p)
```
## Detailed Description
This function performs three critical transformations on index qualification clauses to prepare them for execution:

1. **Qual Clause Selection and Cleanup**: Extracts actual qualification clauses from the input IndexClause list and removes RestrictInfo wrapper nodes, producing clean qualification expressions.

2. **Parameter Replacement**: Replaces outer-relation Var or PlaceHolderVar nodes with nestloop Params to enable proper parameter passing in nested loop joins involving index scans.

3. **Index Key Mapping**: Ensures index keys are represented by Var nodes with varattno set to the index's attribute number rather than the original relation's attribute number, which is essential for correct index access.

The function processes each IndexClause in the index path, iterating through the indexquals within each clause. For each qualification, it creates both a stripped version (with RestrictInfo removed) and a fixed version (with all transformations applied). The fixed version is a complete copy that shares no substructure with the original, which is necessary when subplans are present to avoid execution conflicts.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner context and state
- `*index_path`: IndexPath representing the index scan path being processed
- `**stripped_indexquals_p`: Output parameter receiving list of qual clauses with RestrictInfo removed
- `**fixed_indexquals_p`: Output parameter receiving list of adjusted quals ready for execution
## Dependencies
- Functions called/Symbols referenced:
  - [fix_indexqual_clause](fix_indexqual_clause.md)
  - [IndexPath](../I/IndexPath.md) (struct type)
  - [IndexOptInfo](../I/IndexOptInfo.md) (struct type)  
  - [IndexClause](../I/IndexClause.md) (struct type)
- Called from (representative examples):
  - [create_indexscan_plan](../c/create_indexscan_plan.md)

## Notes and Other Information
This function is a key component in the index scan plan creation process, bridging the gap between the optimizer's representation of index qualifications and the executor's requirements. The comment indicates that parameter replacement responsibility may be moved elsewhere in future versions. The function ensures that both the original and transformed versions of qualifications are available, which is important for various execution scenarios. The complete copying of substructure prevents issues with shared subplan trees during execution. Located in src/backend/optimizer/plan/createplan.c at lines 5023-5063.

## Simplified Source

```c
static void fix_indexqual_references(PlannerInfo *root, IndexPath *index_path,
                                     List **stripped_indexquals_p, List **fixed_indexquals_p) {
    IndexOptInfo *index = index_path->indexinfo;
    List *stripped_indexquals = NIL;
    List *fixed_indexquals = NIL;
    ListCell *lc;

    // Process each IndexClause in the path
    foreach(lc, index_path->indexclauses) {
        IndexClause *iclause = lfirst_node(IndexClause, lc);
        int indexcol = iclause->indexcol;
        ListCell *lc2;

        // Process each qualification within the IndexClause
        foreach(lc2, iclause->indexquals) {
            RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);
            Node *clause = (Node *) rinfo->clause;

            // Create stripped version (remove RestrictInfo wrapper)
            stripped_indexquals = lappend(stripped_indexquals, clause);

            // Create fixed version (apply all transformations)
            clause = fix_indexqual_clause(root, index, indexcol, clause, iclause->indexcols);
            fixed_indexquals = lappend(fixed_indexquals, clause);
        }
    }

    *stripped_indexquals_p = stripped_indexquals;
    *fixed_indexquals_p = fixed_indexquals;
}
```