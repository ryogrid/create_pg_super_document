# generate_append_tlist

## Location
[src/backend/optimizer/prep/prepunion.c:1546-1673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L1546-L1673)

## Overview
Generates a targetlist for a set-operation Append node, creating simple Var nodes with appropriate datatypes, typmods, and collations for combining multiple input relations.

## Definition

```c
static List *
generate_append_tlist(List *colTypes, List *colCollations,
					  bool flag,
					  List *input_tlists,
					  List *refnames_tlist)
```
## Detailed Description
This function constructs a targetlist for Append plan nodes used in set operations by creating simple Var expressions that reference columns from the input subplans. Unlike generate_setop_tlist, this function creates Vars with varno 0 and focuses on determining the appropriate typmod for each column by examining all input targetlists. If all input subplans agree on both the datatype and typmod for a column, that typmod is used; otherwise, typmod is set to -1 to indicate unknown/variable precision.

The function first analyzes all input targetlists to determine the most appropriate typmod for each output column, then constructs the output targetlist with the determined datatypes, typmods, and collations. All entries are simple Vars that will be resolved during execution to reference the appropriate input subplan columns.

## Parameters / Member Variables
- `*colTypes`: OID list of the set-operation's result column datatypes
- `*colCollations`: OID list of the set-operation's result column collations
- `flag`: true to create a resjunk flag column copied up from subplans
- `*input_tlists`: list of targetlists for sub-plans of the Append node
- `*refnames_tlist`: targetlist to take column names from
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [list_length](../l/list_length.md)
  - [list_head](../l/list_head.md)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [lnext](../l/lnext.md)
  - [makeVar](../m/makeVar.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [lappend](../l/lappend.md)
  - [pfree](../p/pfree.md)
  - forthree (macro for iterating over three lists)
- Called from:
  - [generate_recursion_path](generate_recursion_path.md)
  - [generate_union_paths](generate_union_paths.md)  
  - [generate_nonunion_paths](generate_nonunion_paths.md)

## Notes and Other Information
- All generated Vars use varno 0, which indicates they reference the current plan node's output
- Typmod determination is conservative: disagreement among inputs forces typmod to -1
- The function follows the same convention as generate_setop_tlist by setting ressortgroupref equal to resno for all non-resjunk columns
- The flag column, when requested, is created as a resjunk Var that references a flag column from the input subplans
- A known limitation is that set_pathtarget_cost_width cannot determine realistic width estimates for the varno-zero targetlist produced by this function

## Simplified Source

```c
static List *
generate_append_tlist(List *colTypes, List *colCollations,
                     bool flag,
                     List *input_tlists,
                     List *refnames_tlist)
{
    List       *tlist = NIL;
    int         resno = 1;
    int32      *colTypmods;
    int         colindex;

    // First, determine appropriate typmods for each column
    colTypmods = (int32 *) palloc(list_length(colTypes) * sizeof(int32));

    // Analyze all input targetlists to find common typmods
    ListCell *tlistl;
    foreach(tlistl, input_tlists)
    {
        List *subtlist = (List *) lfirst(tlistl);
        ListCell *subtlistl;
        ListCell *curColType = list_head(colTypes);

        colindex = 0;
        foreach(subtlistl, subtlist)
        {
            TargetEntry *subtle = (TargetEntry *) lfirst(subtlistl);

            if (subtle->resjunk)
                continue;

            if (exprType((Node *) subtle->expr) == lfirst_oid(curColType))
            {
                int32 subtypmod = exprTypmod((Node *) subtle->expr);

                // First subplan: copy typmod, others: compare and set to -1 if different
                if (tlistl == list_head(input_tlists))
                    colTypmods[colindex] = subtypmod;
                else if (subtypmod != colTypmods[colindex])
                    colTypmods[colindex] = -1;
            }
            else
            {
                // Type disagreement forces typmod to -1
                colTypmods[colindex] = -1;
            }

            curColType = lnext(colTypes, curColType);
            colindex++;
        }
    }

    // Build the targetlist for the Append node
    colindex = 0;
    ListCell *curColType, *curColCollation, *ref_tl_item;
    forthree(curColType, colTypes, curColCollation, colCollations,
             ref_tl_item, refnames_tlist)
    {
        Oid colType = lfirst_oid(curColType);
        int32 colTypmod = colTypmods[colindex++];
        Oid colColl = lfirst_oid(curColCollation);
        TargetEntry *reftle = (TargetEntry *) lfirst(ref_tl_item);

        // Create a Var with varno 0 (references current plan node)
        Node *expr = (Node *) makeVar(0, resno, colType, colTypmod, colColl, 0);
        TargetEntry *tle = makeTargetEntry((Expr *) expr, (AttrNumber) resno++,
                                          pstrdup(reftle->resname), false);

        // Set ressortgroupref equal to resno (convention for setop trees)
        tle->ressortgroupref = tle->resno;
        tlist = lappend(tlist, tle);
    }

    // Add optional flag column for distinguishing input sources
    if (flag)
    {
        Node *expr = (Node *) makeVar(0, resno, INT4OID, -1, InvalidOid, 0);
        TargetEntry *tle = makeTargetEntry((Expr *) expr, (AttrNumber) resno++,
                                          pstrdup("flag"), true);
        tlist = lappend(tlist, tle);
    }

    pfree(colTypmods);
    return tlist;
}
```