# checkInsertTargets

## Location
[src/backend/parser/parse_target.c:1015-1119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1015-L1119)

## Overview
Generates a list of INSERT column targets when not supplied or validates supplied column names against the target table, returning both column names and their attribute numbers.

## Definition

```c
List *
checkInsertTargets(ParseState *pstate, List *cols, List **attrnos)
```
## Detailed Description
This function handles the column target list processing for INSERT statements in PostgreSQL's parser. It serves two primary purposes:

1. **Default Column Generation**: When no column list is provided (), it automatically generates a complete list of all non-dropped columns from the target relation.

2. **Column Validation**: When a column list is provided, it validates each column name against the target relation's schema, checks for duplicates, and handles both whole column assignments and partial column assignments (with indirection).

The function maintains two bitmapsets to track column usage:  for complete column assignments and  for partial assignments with indirection. This prevents conflicting assignments like specifying both  and  in the same INSERT statement.

## Parameters / Member Variables
- `*pstate`: ParseState structure containing parsing context and target relation information
- `*cols`: Input list of ResTarget nodes representing the column targets (can be NIL for default behavior)
- `**attrnos`: Output parameter - pointer to a list that will be populated with attribute numbers corresponding to the columns
## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - TupleDescAttr
  - makeNode
  - [pstrdup](../p/pstrdup.md)
  - [lappend](../l/lappend.md)
  - [lappend_int](../l/lappend_int.md)
  - [attnameAttNum](../a/attnameAttNum.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - ereport
- Called from (representative examples):
  - [transformInsertStmt](../t/transformInsertStmt.md) (src/backend/parser/analyze.c:672)
  - [transformMergeStmt](../t/transformMergeStmt.md) (src/backend/parser/parse_merge.c:312)

## Notes and Other Information
- The function ensures that dropped columns (attr->attisdropped) are skipped when generating default column lists
- Duplicate column detection is sophisticated: it allows partial column assignments to the same base column (e.g., ) but prevents mixing whole and partial assignments
- Error reporting includes precise location information for better user experience
- The returned attribute numbers are 1-based, following PostgreSQL's attribute numbering convention
- This function is critical for both explicit and implicit INSERT column handling in PostgreSQL's SQL parser

## Simplified Source

```c
List *
checkInsertTargets(ParseState *pstate, List *cols, List **attrnos)
{
    *attrnos = NIL;

    if (cols == NIL) {
        // Generate default column list for INSERT
        int numcol = RelationGetNumberOfAttributes(pstate->p_target_relation);

        for (int i = 0; i < numcol; i++) {
            Form_pg_attribute attr = TupleDescAttr(pstate->p_target_relation->rd_att, i);

            if (attr->attisdropped)
                continue;

            // Create ResTarget for each non-dropped column
            ResTarget *col = makeNode(ResTarget);
            col->name = pstrdup(NameStr(attr->attname));
            col->indirection = NIL;
            col->val = NULL;
            col->location = -1;
            cols = lappend(cols, col);
            *attrnos = lappend_int(*attrnos, i + 1);
        }
    } else {
        // Validate user-supplied INSERT column list
        Bitmapset *wholecols = NULL;
        Bitmapset *partialcols = NULL;

        foreach(ListCell *tl, cols) {
            ResTarget *col = (ResTarget *) lfirst(tl);

            // Look up column name
            int attrno = attnameAttNum(pstate->p_target_relation, col->name, false);
            if (attrno == InvalidAttrNumber)
                ereport(ERROR, /* column does not exist */);

            // Check for duplicate assignments
            if (col->indirection == NIL) {
                // Whole column assignment
                if (bms_is_member(attrno, wholecols) || bms_is_member(attrno, partialcols))
                    ereport(ERROR, /* duplicate column */);
                wholecols = bms_add_member(wholecols, attrno);
            } else {
                // Partial column assignment
                if (bms_is_member(attrno, wholecols))
                    ereport(ERROR, /* conflicting assignment */);
                partialcols = bms_add_member(partialcols, attrno);
            }

            *attrnos = lappend_int(*attrnos, attrno);
        }
    }

    return cols;
}
```