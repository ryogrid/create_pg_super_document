# get_relation_statistics

## Location
[src/backend/optimizer/util/plancat.c:1470-1575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1470-L1575)

## Overview
Retrieves extended statistics metadata defined on a table and prepares StatisticExtInfo objects containing identifying information for the optimizer's use.

## Definition

```c
static List *
get_relation_statistics(RelOptInfo *rel, Relation relation)
```
## Detailed Description
The  function discovers and processes extended statistics objects defined on a relation, creating  structures that contain metadata needed by the query optimizer. The function does not load the actual statistics data but focuses on preparing the identifying information and expression processing.

For each statistics object found, the function builds a bitmapset of covered columns and processes any expressions defined in the statistics object. Expression processing includes constant evaluation and varno adjustment to ensure compatibility with query planning operations. The function calls  twice for each statistics object - once for inherited statistics and once for non-inherited statistics.

The function handles both column-based and expression-based extended statistics, ensuring that expressions are properly normalized through  and have their variable references updated to match the relation's varno in the current query context.

## Parameters / Member Variables
- `*rel`: RelOptInfo structure representing the relation in the optimizer
- `relation`: The actual relation object to extract statistics information from
## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetStatExtList](../R/RelationGetStatExtList.md)
  - Form_pg_statistic_ext
  - [bms_add_member](../b/bms_add_member.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [eval_const_expressions](../e/eval_const_expressions.md)
  - [fix_opfuncids](../f/fix_opfuncids.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [get_relation_statistics_worker](get_relation_statistics_worker.md)
  - [bms_free](../b/bms_free.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [get_relation_info](get_relation_info.md)

## Notes and Other Information
- This is a static function, not part of the external API
- Only processes statistics objects that have actually been built (verified by the worker function)
- Processes expressions through the same pipeline as qual clauses for proper matching
- Creates separate StatisticExtInfo entries for inherited and non-inherited statistics
- Properly manages memory by freeing temporary data structures
- Uses 1-based column indexing consistent with PostgreSQL conventions
- The function assumes statistics objects exist but may not have actual data built
- Expression processing includes opfuncid fixing for optimization purposes

## Simplified Source

```c
static List *
get_relation_statistics(RelOptInfo *rel, Relation relation)
{
    Index varno = rel->relid;
    List *statoidlist;
    List *stainfos = NIL;
    ListCell *l;

    // Get list of extended statistics objects for this relation
    statoidlist = RelationGetStatExtList(relation);

    foreach(l, statoidlist) {
        Oid statOid = lfirst_oid(l);
        Form_pg_statistic_ext staForm;
        HeapTuple htup;
        Bitmapset *keys = NULL;
        List *exprs = NIL;
        int i;

        // Look up the statistics object in system catalog
        htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statOid));
        if (!HeapTupleIsValid(htup))
            elog(ERROR, "cache lookup failed for statistics object %u", statOid);
        staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);

        // Build bitmapset of covered columns
        for (i = 0; i < staForm->stxkeys.dim1; i++)
            keys = bms_add_member(keys, staForm->stxkeys.values[i]);

        // Process expressions if any exist
        {
            bool isnull;
            Datum datum;

            // Get expression text from catalog
            datum = SysCacheGetAttr(STATEXTOID, htup,
                                    Anum_pg_statistic_ext_stxexprs, &isnull);

            if (!isnull) {
                char *exprsString;

                // Parse and process expressions
                exprsString = TextDatumGetCString(datum);
                exprs = (List *) stringToNode(exprsString);
                pfree(exprsString);

                // Normalize expressions for planner compatibility
                exprs = (List *) eval_const_expressions(NULL, (Node *) exprs);
                fix_opfuncids((Node *) exprs);

                // Adjust variable numbers to match current query context
                if (varno != 1)
                    ChangeVarNodes((Node *) exprs, 1, varno, 0);
            }
        }

        // Create StatisticExtInfo entries for both inherited and non-inherited stats
        get_relation_statistics_worker(&stainfos, rel, statOid, true, keys, exprs);
        get_relation_statistics_worker(&stainfos, rel, statOid, false, keys, exprs);

        // Clean up
        ReleaseSysCache(htup);
        bms_free(keys);
    }

    list_free(statoidlist);
    return stainfos;
}
```