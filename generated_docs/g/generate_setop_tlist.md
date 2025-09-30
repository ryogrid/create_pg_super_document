# generate_setop_tlist

## Location
[src/backend/optimizer/prep/prepunion.c:1397-1545](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L1397-L1545)

## Overview
Generates a targetlist for a set-operation plan node (UNION/INTERSECT/EXCEPT), creating appropriate column references with proper data types and collations.

## Definition

```c
static List *
generate_setop_tlist(List *colTypes, List *colCollations,
					 int flag,
					 Index varno,
					 bool hack_constants,
					 List *input_tlist,
					 List *refnames_tlist,
					 bool *trivial_tlist)
```
## Detailed Description
This function constructs a targetlist for set-operation plan nodes by creating TargetEntry nodes that reference input columns with appropriate data type coercions and collation handling. It ensures that the output columns have the correct datatypes and collations as determined by the set-operation analysis. The function also handles a special case where constants from the input targetlist can be copied directly rather than referenced as subquery outputs, which is important for proper handling of UNKNOWN constants during type coercion.

The function sets all non-resjunk columns to have ressortgroupref equal to their resno by convention, which is used by the set-operation planning logic. It can optionally add a resjunk flag column when needed for distinguishing between different input relations in the set operation.

## Parameters / Member Variables
- : OID list of the set-operation's result column datatypes
- : OID list of the set-operation's result column collations  
- : -1 if no flag column needed, 0 or 1 to create a const flag column
- : varno to use in generated Vars that reference input columns
- : true to copy up constants directly rather than referencing them
- : targetlist of this node's input node
- : targetlist to take column names from
- : output parameter, set to true if resulting targetlist is trivial

## Dependencies
- Functions called/Symbols referenced:
  - [makeVar](../m/makeVar.md)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)  
  - [exprCollation](../e/exprCollation.md)
  - [coerce_to_common_type](../c/coerce_to_common_type.md)
  - [applyRelabelType](../a/applyRelabelType.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [makeConst](../m/makeConst.md)
  - forfour (macro for iterating over four lists)
- Called from:
  - [recurse_set_operations](../r/recurse_set_operations.md)

## Notes and Other Information
- The function marks the tlist as non-trivial when type coercions or collation relabeling is required
- Constants are handled specially via the hack_constants parameter to ensure proper UNKNOWN constant handling
- All non-resjunk columns get ressortgroupref set to their resno for set-operation planning consistency
- The flag column, when added, is always marked as resjunk and contains a constant integer value
- Type coercions use coerce_to_common_type while collation adjustments use applyRelabelType with RelabelType nodes

## Simplified Source

```c
static List *
generate_setop_tlist(List *colTypes, List *colCollations, int flag,
                     Index varno, bool hack_constants,
                     List *input_tlist, List *refnames_tlist,
                     bool *trivial_tlist)
{
    List *tlist = NIL;
    int resno = 1;
    ListCell *ctlc, *cclc, *itlc, *rtlc;

    *trivial_tlist = true;

    // Process each column type, collation, input and reference entry together
    forfour(ctlc, colTypes, cclc, colCollations, itlc, input_tlist, rtlc, refnames_tlist)
    {
        Oid colType = lfirst_oid(ctlc);
        Oid colColl = lfirst_oid(cclc);
        TargetEntry *inputtle = (TargetEntry *) lfirst(itlc);
        TargetEntry *reftle = (TargetEntry *) lfirst(rtlc);
        Node *expr;

        // Create expression: either copy constant or create variable reference
        if (hack_constants && inputtle->expr && IsA(inputtle->expr, Const))
            expr = (Node *) inputtle->expr;
        else
            expr = (Node *) makeVar(varno, inputtle->resno,
                                  exprType((Node *) inputtle->expr),
                                  exprTypmod((Node *) inputtle->expr),
                                  exprCollation((Node *) inputtle->expr), 0);

        // Add type coercion if needed
        if (exprType(expr) != colType) {
            expr = coerce_to_common_type(NULL, expr, colType, "UNION/INTERSECT/EXCEPT");
            *trivial_tlist = false;
        }

        // Add collation relabeling if needed
        if (exprCollation(expr) != colColl) {
            expr = applyRelabelType(expr, exprType(expr), exprTypmod(expr), colColl,
                                  COERCE_IMPLICIT_CAST, -1, false);
            *trivial_tlist = false;
        }

        // Create target entry with sort group reference
        TargetEntry *tle = makeTargetEntry((Expr *) expr, (AttrNumber) resno++,
                                         pstrdup(reftle->resname), false);
        tle->ressortgroupref = tle->resno;
        tlist = lappend(tlist, tle);
    }

    // Add optional flag column
    if (flag >= 0) {
        Node *expr = (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
                                       Int32GetDatum(flag), false, true);
        TargetEntry *tle = makeTargetEntry((Expr *) expr, (AttrNumber) resno++,
                                         pstrdup("flag"), true);
        tlist = lappend(tlist, tle);
        *trivial_tlist = false;
    }

    return tlist;
}
```