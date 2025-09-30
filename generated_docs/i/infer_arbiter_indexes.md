# infer_arbiter_indexes

## Location
[src/backend/optimizer/util/plancat.c:705-977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L705-L977)

## Overview
Determines the unique indexes used to arbitrate speculative insertion for ON CONFLICT clauses by matching user-supplied inference specifications against available unique indexes.

## Definition

```c
List *
infer_arbiter_indexes(PlannerInfo *root)
```
## Detailed Description
This function implements the core logic for PostgreSQL's ON CONFLICT clause by identifying which unique indexes should be used for conflict detection during speculative insertion. It takes the inference specification from an OnConflictExpr and matches it against the unique indexes defined on the target relation.

The matching process involves several steps:
1. Parse arbiter elements (columns/expressions) from the ON CONFLICT clause
2. Build normalized representations of both plain attributes and expressions
3. Handle named constraint specifications by looking up the associated index
4. Iterate through all available indexes to find exact matches on columns/expressions
5. Verify collation and operator class compatibility via infer_collation_opclass_match
6. Ensure partial index predicates are implied by the WHERE clause
7. Return a list of matching index OIDs for conflict resolution

The function requires exact matches on indexed columns/expressions but allows flexible ordering. For partial indexes, the predicate must be logically implied by the ON CONFLICT WHERE clause.

## Parameters / Member Variables
- : PlannerInfo structure containing the parsed query with OnConflictExpr information

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - [table_open](../t/table_open.md), table_close
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [index_open](index_open.md), index_close
  - [get_constraint_index](../g/get_constraint_index.md)
  - [infer_collation_opclass_match](infer_collation_opclass_match.md)
  - [RelationGetIndexExpressions](../R/RelationGetIndexExpressions.md)
  - [RelationGetIndexPredicate](../R/RelationGetIndexPredicate.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [bms_add_member](../b/bms_add_member.md), bms_equal
  - [list_member](../l/list_member.md), list_difference
- Called from (representative examples):
  - [make_modifytable](../m/make_modifytable.md)

## Notes and Other Information
- Returns NIL for ON CONFLICT DO NOTHING without inference specification
- Does not consider indcheckxmin for candidate elimination (unlike get_relation_info)
- Supports both named constraints and inference element specifications
- Requires exact expression matching but allows flexible attribute ordering
- Validates that partial index predicates are implied by ON CONFLICT WHERE clause
- Raises errors for unsupported features like whole-row inference specifications
- Used specifically for UPSERT operations and conflict resolution in INSERT statements

## Simplified Source

```c
List *
infer_arbiter_indexes(PlannerInfo *root)
{
    OnConflictExpr *onconflict = root->parse->onConflict;
    Index varno;
    RangeTblEntry *rte;
    Relation relation;
    Oid indexOidFromConstraint = InvalidOid;
    List *indexList;

    // Normalized inference attributes and expressions
    Bitmapset *inferAttrs = NULL;
    List *inferElems = NIL;
    List *results = NIL;

    // Quick return for DO NOTHING without inference specification
    if (onconflict->arbiterElems == NIL && onconflict->constraint == InvalidOid)
        return NIL;

    // Get target relation
    varno = root->parse->resultRelation;
    rte = rt_fetch(varno, root->parse->rtable);
    relation = table_open(rte->relid, NoLock);

    // Build normalized representation of arbiter elements
    foreach(ListCell *l, onconflict->arbiterElems)
    {
        InferenceElem *elem = (InferenceElem *) lfirst(l);

        if (!IsA(elem->expr, Var))
        {
            // Non-Var expressions go into inferElems list
            inferElems = lappend(inferElems, elem->expr);
            continue;
        }

        Var *var = (Var *) elem->expr;
        int attno = var->varattno;

        if (attno == 0)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("whole row unique index inference specifications are not supported")));

        // Add attribute to bitmap
        inferAttrs = bms_add_member(inferAttrs, attno - FirstLowInvalidHeapAttributeNumber);
    }

    // Handle named constraint case
    if (onconflict->constraint != InvalidOid)
    {
        indexOidFromConstraint = get_constraint_index(onconflict->constraint);
        if (indexOidFromConstraint == InvalidOid)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("constraint in ON CONFLICT clause has no associated index")));
    }

    // Search through all indexes on the target relation
    indexList = RelationGetIndexList(relation);

    foreach(ListCell *l, indexList)
    {
        Oid indexoid = lfirst_oid(l);
        Relation idxRel = index_open(indexoid, rte->rellockmode);
        Form_pg_index idxForm = idxRel->rd_index;

        // Skip invalid indexes
        if (!idxForm->indisvalid)
            goto next;

        // Handle named constraint case
        if (indexOidFromConstraint == idxForm->indexrelid)
        {
            if (!idxForm->indisunique && onconflict->action == ONCONFLICT_UPDATE)
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("ON CONFLICT DO UPDATE not supported with exclusion constraints")));

            results = lappend_oid(results, idxForm->indexrelid);
            break; // Found the named constraint index
        }
        else if (indexOidFromConstraint != InvalidOid)
        {
            goto next; // Skip other indexes when looking for named constraint
        }

        // For inference elements, only consider unique indexes
        if (!idxForm->indisunique)
            goto next;

        // Build bitmap of indexed attributes
        Bitmapset *indexedAttrs = NULL;
        for (AttrNumber natt = 0; natt < idxForm->indnkeyatts; natt++)
        {
            int attno = idxRel->rd_index->indkey.values[natt];
            if (attno != 0)
                indexedAttrs = bms_add_member(indexedAttrs,
                    attno - FirstLowInvalidHeapAttributeNumber);
        }

        // Check if non-expression attributes match
        if (!bms_equal(indexedAttrs, inferAttrs))
            goto next;

        // Check expression attributes and collations/opclasses
        List *idxExprs = RelationGetIndexExpressions(idxRel);
        if (idxExprs && varno != 1)
            ChangeVarNodes((Node *) idxExprs, 1, varno, 0);

        bool match = true;
        foreach(ListCell *el, onconflict->arbiterElems)
        {
            InferenceElem *elem = (InferenceElem *) lfirst(el);

            // Check collation/opclass compatibility
            if (!infer_collation_opclass_match(elem, idxRel, idxExprs))
            {
                match = false;
                break;
            }

            // Skip Vars (already handled above)
            if (IsA(elem->expr, Var))
                continue;

            // Check if expression matches
            if (elem->infercollid == InvalidOid &&
                elem->inferopclass == InvalidOid &&
                !list_member(idxExprs, elem->expr))
            {
                match = false;
                break;
            }
        }

        if (!match)
            goto next;

        // Ensure all index expressions are covered
        if (list_difference(idxExprs, inferElems) != NIL)
            goto next;

        // Check partial index predicate
        List *predExprs = RelationGetIndexPredicate(idxRel);
        if (predExprs && varno != 1)
            ChangeVarNodes((Node *) predExprs, 1, varno, 0);

        if (!predicate_implied_by(predExprs, (List *) onconflict->arbiterWhere, false))
            goto next;

        // This index matches!
        results = lappend_oid(results, idxForm->indexrelid);

next:
        index_close(idxRel, NoLock);
    }

    // Clean up
    list_free(indexList);
    table_close(relation, NoLock);

    // Error if no matching indexes found
    if (results == NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
            errmsg("there is no unique or exclusion constraint matching the ON CONFLICT specification")));

    return results;
}
```