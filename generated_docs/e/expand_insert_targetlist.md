# expand_insert_targetlist

## Location
[src/backend/optimizer/prep/preptlist.c:382-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/preptlist.c#L382-L525)

## Overview
Expands an INSERT targetlist to include entries for missing table attributes and ensures non-junk attributes appear in proper field order to match the target relation's structure.

## Definition
```c
static List *expand_insert_targetlist(PlannerInfo *root, List *tlist, Relation rel)
```

## Detailed Description
The `expand_insert_targetlist` function takes a parser-generated targetlist for an INSERT statement and transforms it to match exactly the structure expected by the executor. The executor requires that the targetlist contain entries for every attribute in the target table, in the exact order they appear in the table definition.

The function scans through each attribute in the target relation and either:
1. Uses the existing targetlist entry if one exists for that attribute
2. Creates a new NULL-valued targetlist entry if the attribute is missing from the original targetlist

Special handling is provided for different column types:
- **Dropped columns**: Inserts a NULL constant with INT4 type (since the original datatype may no longer exist)
- **Generated columns**: Inserts a NULL of the base type without domain constraints to avoid errors
- **Normal columns**: Uses `coerce_null_to_domain` to create a properly-typed NULL that respects domain constraints

After processing all table attributes, any remaining resjunk (auxiliary) entries from the original targetlist are appended with properly renumbered resnos.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and state
- `tlist`: Original targetlist from the parser
- `rel`: Target relation for the INSERT operation

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - TupleDescAttr
  - [makeConst](../m/makeConst.md)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)
  - [coerce_null_to_domain](../c/coerce_null_to_domain.md)
  - [eval_const_expressions](eval_const_expressions.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [flatCopyTargetEntry](../f/flatCopyTargetEntry.md)
  - [list_head](../l/list_head.md)
  - [lnext](../l/lnext.md)
- Called from (representative examples):
  - [preprocess_targetlist](../p/preprocess_targetlist.md) (src/backend/optimizer/prep/preptlist.c:107, 153)

## Notes and Other Information
This function is located in src/backend/optimizer/prep/preptlist.c:382-525 and is declared as static, meaning it's only used within the same file. It's a critical component for INSERT statement processing, ensuring that the executor receives a complete and correctly ordered targetlist. The function handles various PostgreSQL-specific features like dropped columns, generated columns, and domain constraints while maintaining compatibility with the executor's expectations.

## Simplified Source

```c
static List *expand_insert_targetlist(PlannerInfo *root, List *tlist, Relation rel)
{
    List *new_tlist = NIL;
    ListCell *tlist_item;
    int attrno, numattrs;

    tlist_item = list_head(tlist);
    numattrs = RelationGetNumberOfAttributes(rel);

    // Process each attribute in the target relation
    for (attrno = 1; attrno <= numattrs; attrno++)
    {
        Form_pg_attribute att_tup = TupleDescAttr(rel->rd_att, attrno - 1);
        TargetEntry *new_tle = NULL;

        // Check if we have an existing targetlist entry for this column
        if (tlist_item != NULL)
        {
            TargetEntry *old_tle = (TargetEntry *) lfirst(tlist_item);

            if (!old_tle->resjunk && old_tle->resno == attrno)
            {
                new_tle = old_tle;  // Use existing entry
                tlist_item = lnext(tlist, tlist_item);
            }
        }

        // Create missing targetlist entry with appropriate NULL value
        if (new_tle == NULL)
        {
            Node *new_expr;

            if (att_tup->attisdropped)
            {
                // Dropped column: use INT4 NULL (original type may not exist)
                new_expr = (Node *) makeConst(INT4OID, -1, InvalidOid,
                                              sizeof(int32), (Datum) 0,
                                              true, true);
            }
            else if (att_tup->attgenerated)
            {
                // Generated column: use base type NULL (no domain constraints)
                Oid baseTypeId = att_tup->atttypid;
                int32 baseTypeMod = att_tup->atttypmod;

                baseTypeId = getBaseTypeAndTypmod(baseTypeId, &baseTypeMod);
                new_expr = (Node *) makeConst(baseTypeId, baseTypeMod,
                                              att_tup->attcollation, att_tup->attlen,
                                              (Datum) 0, true, att_tup->attbyval);
            }
            else
            {
                // Normal column: apply domain constraints
                new_expr = coerce_null_to_domain(att_tup->atttypid,
                                                 att_tup->atttypmod,
                                                 att_tup->attcollation,
                                                 att_tup->attlen,
                                                 att_tup->attbyval);
                if (!IsA(new_expr, Const))
                    new_expr = eval_const_expressions(root, new_expr);
            }

            new_tle = makeTargetEntry((Expr *) new_expr, attrno,
                                      pstrdup(NameStr(att_tup->attname)), false);
        }

        new_tlist = lappend(new_tlist, new_tle);
    }

    // Append remaining resjunk entries with proper numbering
    while (tlist_item)
    {
        TargetEntry *old_tle = (TargetEntry *) lfirst(tlist_item);

        if (!old_tle->resjunk)
            elog(ERROR, "targetlist is not sorted correctly");

        // Renumber if necessary
        if (old_tle->resno != attrno)
        {
            old_tle = flatCopyTargetEntry(old_tle);
            old_tle->resno = attrno;
        }

        new_tlist = lappend(new_tlist, old_tle);
        attrno++;
        tlist_item = lnext(tlist, tlist_item);
    }

    return new_tlist;
}
```