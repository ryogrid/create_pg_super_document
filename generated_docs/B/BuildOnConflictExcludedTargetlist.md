# BuildOnConflictExcludedTargetlist

## Location
[src/backend/parser/analyze.c:1225-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1225-L1294)

## Overview
Creates a target list for the EXCLUDED pseudo-relation used in ON CONFLICT clauses, representing all columns of the target relation with proper variable references.

## Definition
```c
List *
BuildOnConflictExcludedTargetlist(Relation targetrel, Index exclRelIndex)
```

## Detailed Description
This function constructs a target list that represents the EXCLUDED pseudo-relation in ON CONFLICT DO UPDATE clauses. The EXCLUDED relation provides access to the values that would have been inserted if there had been no conflict.

Key aspects of the implementation:
- **Complete column coverage**: Creates entries for all columns including dropped ones to maintain proper attribute number correspondence
- **Dropped column handling**: Uses NULL constants for dropped columns since they cannot be referenced
- **Whole-row variable**: Adds a special entry to support "EXCLUDED.*" references
- **Attribute number alignment**: Ensures target entry result numbers match attribute numbers for proper resolution

The function handles the special requirements of the EXCLUDED relation:
- Must include entries for dropped columns to maintain resno/attno correspondence
- Uses the provided exclRelIndex for all variable references
- Creates a non-standard target list where resno values match varattno values
- Supports both individual column references and whole-row references

## Parameters / Member Variables
- `targetrel`: The target relation of the INSERT statement whose structure determines the EXCLUDED relation layout
- `exclRelIndex`: The range table index assigned to the EXCLUDED pseudo-relation

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes (gets column count from relation)
  - [makeNullConst](../m/makeNullConst.md) (creates NULL constants for dropped columns)
  - [makeVar](../m/makeVar.md) (creates variable references for active columns)
  - [makeTargetEntry](../m/makeTargetEntry.md) (creates target list entries)
  - TupleDescAttr (accesses attribute metadata)

- Called from (representative examples):
  - [transformOnConflictClause](../t/transformOnConflictClause.md) (during ON CONFLICT processing)
  - [rewriteTargetView](../r/rewriteTargetView.md) (for view rewriting with ON CONFLICT)

## Notes and Other Information
- Exported function available to the rewriter for view processing
- Creates a non-standard target list where resno matches varattno instead of sequential numbering
- Critical for proper resolution of EXCLUDED column references in UPDATE expressions
- The whole-row variable enables "EXCLUDED.*" syntax in ON CONFLICT DO UPDATE
- Maintains correspondence with actual table structure including dropped columns
- Used primarily for EXPLAIN plan generation and reference resolution

## Simplified Source

```c
List *
BuildOnConflictExcludedTargetlist(Relation targetrel, Index exclRelIndex)
{
    List       *result = NIL;
    int         attno;
    Var        *var;
    TargetEntry *te;

    // Create target entries for all columns (including dropped ones)
    for (attno = 0; attno < RelationGetNumberOfAttributes(targetrel); attno++)
    {
        Form_pg_attribute attr = TupleDescAttr(targetrel->rd_att, attno);
        char       *name;

        if (attr->attisdropped)
        {
            // Use NULL constant for dropped columns
            var = (Var *) makeNullConst(INT4OID, -1, InvalidOid);
            name = NULL;
        }
        else
        {
            // Create Var reference for active columns
            var = makeVar(exclRelIndex, attno + 1,
                         attr->atttypid, attr->atttypmod,
                         attr->attcollation, 0);
            name = pstrdup(NameStr(attr->attname));
        }

        te = makeTargetEntry((Expr *) var, attno + 1, name, false);
        result = lappend(result, te);
    }

    // Add whole-row variable for "EXCLUDED.*" support
    var = makeVar(exclRelIndex, InvalidAttrNumber,
                 targetrel->rd_rel->reltype, -1, InvalidOid, 0);
    te = makeTargetEntry((Expr *) var, InvalidAttrNumber, NULL, true);
    result = lappend(result, te);

    return result;
}
```