# checkInsertTargets

## Location
src/backend/parser/parse_target.c: 1015 - 1119

## Overview
Generates a list of INSERT column targets when not supplied or validates supplied column names against the target table, returning both column names and their attribute numbers.

## Definition


## Detailed Description
This function handles the column target list processing for INSERT statements in PostgreSQL's parser. It serves two primary purposes:

1. **Default Column Generation**: When no column list is provided (), it automatically generates a complete list of all non-dropped columns from the target relation.

2. **Column Validation**: When a column list is provided, it validates each column name against the target relation's schema, checks for duplicates, and handles both whole column assignments and partial column assignments (with indirection).

The function maintains two bitmapsets to track column usage:  for complete column assignments and  for partial assignments with indirection. This prevents conflicting assignments like specifying both  and  in the same INSERT statement.

## Parameters / Member Variables
- : ParseState structure containing parsing context and target relation information
- : Input list of ResTarget nodes representing the column targets (can be NIL for default behavior)
- : Output parameter - pointer to a list that will be populated with attribute numbers corresponding to the columns

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - TupleDescAttr
  - makeNode
  - pstrdup
  - lappend
  - lappend_int
  - attnameAttNum
  - bms_is_member
  - bms_add_member
  - ereport
- Called from (representative examples):
  - transformInsertStmt (src/backend/parser/analyze.c:672)
  - transformMergeStmt (src/backend/parser/parse_merge.c:312)

## Notes and Other Information
- The function ensures that dropped columns (attr->attisdropped) are skipped when generating default column lists
- Duplicate column detection is sophisticated: it allows partial column assignments to the same base column (e.g., ) but prevents mixing whole and partial assignments
- Error reporting includes precise location information for better user experience
- The returned attribute numbers are 1-based, following PostgreSQL's attribute numbering convention
- This function is critical for both explicit and implicit INSERT column handling in PostgreSQL's SQL parser