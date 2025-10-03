# rewriteValuesRTE

## Location
[src/backend/rewrite/rewriteHandler.c:1403-1587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1403-L1587)

## Overview
Handles DEFAULT value replacement in VALUES RTEs during INSERT statement rewriting, replacing DEFAULT items with appropriate default expressions or NULL values.

## Definition

```c
static bool
rewriteValuesRTE(Query *parsetree, RangeTblEntry *rte, int rti,
				 Relation target_relation,
				 Bitmapset *unused_cols)
```
## Detailed Description
This function processes INSERT ... VALUES statements with multiple VALUES lists (VALUES RTE) and replaces any DEFAULT items with the appropriate default expressions. The function handles different scenarios based on the target relation type:

- For auto-updatable views: DEFAULT items are replaced with the view's default if available, otherwise left untouched for the underlying base relation to handle
- For other relation types (including rule- and trigger-updatable views): All DEFAULT items are replaced, setting to NULL if no default exists
- For columns in unused_cols: DEFAULT items are explicitly set to NULL regardless of relation type

The function performs optimization by first scanning for DEFAULT placeholders to avoid unnecessary processing if none exist.

## Parameters / Member Variables
- `*parsetree`: The INSERT query being rewritten
- `*rte`: The VALUES range table entry containing the VALUES lists
- `rti`: Range table index of the VALUES RTE
- `target_relation`: The target relation for the INSERT operation
- `*unused_cols`: Bitmapset of column numbers that are no longer used in the targetlist
## Dependencies
- Functions called/Symbols referenced:
  - [searchForDefault](../s/searchForDefault.md)
  - [matchLocks](../m/matchLocks.md)
  - [view_has_instead_trigger](../v/view_has_instead_trigger.md)
  - [build_column_default](../b/build_column_default.md)
  - [makeNullConst](../m/makeNullConst.md)
  - [coerce_null_to_domain](../c/coerce_null_to_domain.md)
  - [bms_is_member](../b/bms_is_member.md)
- Called from:
  - [RewriteQuery](../R/RewriteQuery.md)

## Notes and Other Information
- Only processes INSERT commands with VALUES RTEs
- Returns true if all DEFAULT items were replaced, false if some were left untouched (auto-updatable views)
- Handles subscripted or field assignment targetlist entries from already-replaced DEFAULT items in recursive calls
- Performs validation to ensure DEFAULT items only appear in appropriate contexts
- Uses expensive list rebuilding only when DEFAULT placeholders are actually present

## Simplified Source

```c
static bool rewriteValuesRTE(Query *parsetree, RangeTblEntry *rte, int rti,
                            Relation target_relation, Bitmapset *unused_cols)
{
    List *newValues;
    ListCell *lc;
    bool isAutoUpdatableView;
    bool allReplaced;
    int numattrs;
    int *attrnos;

    Assert(parsetree->commandType == CMD_INSERT);
    Assert(rte->rtekind == RTE_VALUES);

    // Quick check: if no DEFAULT placeholders exist, nothing to do
    if (!searchForDefault(rte))
        return true;

    // Map target list entries to VALUES columns
    numattrs = list_length(linitial(rte->values_lists));
    attrnos = (int *) palloc0(numattrs * sizeof(int));

    foreach(lc, parsetree->targetList)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(lc);

        if (IsA(tle->expr, Var))
        {
            Var *var = (Var *) tle->expr;
            if (var->varno == rti)
            {
                int attrno = var->varattno;
                Assert(attrno >= 1 && attrno <= numattrs);
                attrnos[attrno - 1] = tle->resno;
            }
        }
    }

    // Check if target is an auto-updatable view
    isAutoUpdatableView = false;
    if (target_relation->rd_rel->relkind == RELKIND_VIEW &&
        !view_has_instead_trigger(target_relation, CMD_INSERT, NIL))
    {
        List *locks;
        bool hasUpdate;
        bool found;
        ListCell *l;

        // Look for unconditional DO INSTEAD rule
        locks = matchLocks(CMD_INSERT, target_relation,
                          parsetree->resultRelation, parsetree, &hasUpdate);

        found = false;
        foreach(l, locks)
        {
            RewriteRule *rule_lock = (RewriteRule *) lfirst(l);
            if (rule_lock->isInstead && rule_lock->qual == NULL)
            {
                found = true;
                break;
            }
        }

        // No unconditional DO INSTEAD rule means auto-updatable
        if (!found)
            isAutoUpdatableView = true;
    }

    // Process each VALUES list
    newValues = NIL;
    allReplaced = true;
    foreach(lc, rte->values_lists)
    {
        List *sublist = (List *) lfirst(lc);
        List *newList = NIL;
        ListCell *lc2;
        int i;

        Assert(list_length(sublist) == numattrs);

        i = 0;
        foreach(lc2, sublist)
        {
            Node *col = (Node *) lfirst(lc2);
            int attrno = attrnos[i++];

            if (IsA(col, SetToDefault))
            {
                Form_pg_attribute att_tup;
                Node *new_expr;

                // Handle unused columns - set to NULL
                if (bms_is_member(i, unused_cols))
                {
                    SetToDefault *def = (SetToDefault *) col;
                    newList = lappend(newList,
                                    makeNullConst(def->typeId,
                                                 def->typeMod,
                                                 def->collation));
                    continue;
                }

                if (attrno == 0)
                    elog(ERROR, "cannot set value in column %d to DEFAULT", i);

                Assert(attrno > 0 && attrno <= target_relation->rd_att->natts);
                att_tup = TupleDescAttr(target_relation->rd_att, attrno - 1);

                // Get column default expression
                if (!att_tup->attisdropped)
                    new_expr = build_column_default(target_relation, attrno);
                else
                    new_expr = NULL;  // Force NULL for dropped columns

                // Handle missing defaults
                if (!new_expr)
                {
                    if (isAutoUpdatableView)
                    {
                        // Leave untouched for auto-updatable views
                        newList = lappend(newList, col);
                        allReplaced = false;
                        continue;
                    }

                    // Create NULL with proper domain coercion
                    new_expr = coerce_null_to_domain(att_tup->atttypid,
                                                   att_tup->atttypmod,
                                                   att_tup->attcollation,
                                                   att_tup->attlen,
                                                   att_tup->attbyval);
                }
                newList = lappend(newList, new_expr);
            }
            else
                newList = lappend(newList, col);
        }
        newValues = lappend(newValues, newList);
    }

    rte->values_lists = newValues;
    pfree(attrnos);

    return allReplaced;
}
```