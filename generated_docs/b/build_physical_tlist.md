# build_physical_tlist

## Location
[src/backend/optimizer/util/plancat.c:1764-1884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1764-L1884)

## Overview
Constructs a target list consisting of exactly the relation's user attributes in order, enabling executor optimizations by avoiding projection steps at runtime.

## Definition

```c
List *
build_physical_tlist(PlannerInfo *root, RelOptInfo *rel)
```
## Detailed Description
This function builds a "physical" target list that matches the actual physical layout of a relation's columns. The executor can special-case such target lists to avoid projection operations, providing significant performance benefits for scan nodes. The function handles multiple types of range table entries including base relations, subqueries, functions, values lists, CTEs, and other table expressions.

The function creates a target list where each entry corresponds to a relation attribute in its natural order. However, it applies a conservative approach: if any dropped columns or columns with missing values are detected, it returns NIL (empty list) to punt the optimization. This avoids complications with type information that may no longer be available for dropped columns.

For different RTE kinds:
- **RTE_RELATION**: Iterates through relation attributes, creating Var nodes for each
- **RTE_SUBQUERY**: Maps subquery target list entries to Var nodes
- **RTE_FUNCTION/TABLEFUNC/VALUES/CTE/etc**: Uses expandRTE to get column information

## Parameters / Member Variables
- `*root`: PlannerInfo containing global planner state and range table information
- `*rel`: RelOptInfo representing the relation for which to build the physical target list
## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch (retrieves range table entry by index)
  - [table_open](../t/table_open.md)/table_close (relation access functions)
  - RelationGetNumberOfAttributes (gets attribute count)
  - [makeVar](../m/makeVar.md) (creates Var nodes for table columns)
  - [makeTargetEntry](../m/makeTargetEntry.md) (creates target list entries)
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md) (creates Var from subquery target entry)
  - [expandRTE](../e/expandRTE.md) (expands range table entry to column list)
  - TupleDescAttr (accesses tuple descriptor attributes)
  - RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION, etc. (range table entry kinds)

- Called from (representative examples):
  - [create_scan_plan](../c/create_scan_plan.md) (src/backend/optimizer/plan/createplan.c:659)

## Notes and Other Information
- Returns NIL when dropped columns (attisdropped) or missing columns (atthasmissing) are encountered
- Supports optimization for various scan node types: SeqScan, SubqueryScan, FunctionScan, ValuesScan, CteScan, etc.
- Critical for avoiding unnecessary projection overhead in the executor
- The optimization is especially valuable for wide tables where projection costs would be significant
- Conservative approach ensures type safety by avoiding issues with dropped column types
- Location: src/backend/optimizer/util/plancat.c:1764-1884

## Simplified Source

```c
List *build_physical_tlist(PlannerInfo *root, RelOptInfo *rel) {
    List *tlist = NIL;
    Index varno = rel->relid;
    RangeTblEntry *rte = planner_rt_fetch(varno, root);

    switch (rte->rtekind) {
        case RTE_RELATION: {
            // Handle base relations - check for dropped/missing columns
            Relation relation = table_open(rte->relid, NoLock);
            int numattrs = RelationGetNumberOfAttributes(relation);

            for (int attrno = 1; attrno <= numattrs; attrno++) {
                Form_pg_attribute att_tup = TupleDescAttr(relation->rd_att, attrno - 1);

                // Punt if any columns are dropped or have missing values
                if (att_tup->attisdropped || att_tup->atthasmissing) {
                    tlist = NIL;
                    break;
                }

                // Create Var and TargetEntry for this column
                Var *var = makeVar(varno, attrno, att_tup->atttypid,
                                  att_tup->atttypmod, att_tup->attcollation, 0);
                tlist = lappend(tlist, makeTargetEntry((Expr *) var, attrno, NULL, false));
            }
            table_close(relation, NoLock);
            break;
        }

        case RTE_SUBQUERY: {
            // Handle subqueries - map target list entries to Vars
            Query *subquery = rte->subquery;
            foreach(l, subquery->targetList) {
                TargetEntry *tle = lfirst(l);
                Var *var = makeVarFromTargetEntry(varno, tle);
                tlist = lappend(tlist, makeTargetEntry((Expr *) var, tle->resno, NULL, tle->resjunk));
            }
            break;
        }

        case RTE_FUNCTION:
        case RTE_TABLEFUNC:
        case RTE_VALUES:
        case RTE_CTE:
        case RTE_NAMEDTUPLESTORE:
        case RTE_RESULT: {
            // Handle other RTE kinds using expandRTE
            List *colvars;
            expandRTE(rte, varno, 0, -1, true, NULL, &colvars);

            foreach(l, colvars) {
                Var *var = lfirst(l);

                // Punt if expandRTE returned a non-Var (dropped column)
                if (!IsA(var, Var)) {
                    tlist = NIL;
                    break;
                }

                tlist = lappend(tlist, makeTargetEntry((Expr *) var, var->varattno, NULL, false));
            }
            break;
        }

        default:
            elog(ERROR, "unsupported RTE kind %d in build_physical_tlist", (int) rte->rtekind);
    }

    return tlist;
}
```