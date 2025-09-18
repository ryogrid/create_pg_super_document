# transformUpdateTargetList

## Location
src/backend/parser/analyze.c: 2485 - 2559

## Overview
Handles the SET clause transformation in UPDATE, MERGE, and INSERT...ON CONFLICT UPDATE statements by converting target expressions to proper target list entries with correct column assignments and permission tracking.

## Definition
List *transformUpdateTargetList(ParseState *pstate, List *origTlist)

## Detailed Description
This function transforms the SET clause target list for update operations, ensuring proper column resolution, permission tracking, and target entry structure. The process involves several key steps:

1. Transforms the original target list using standard target list processing with UPDATE_SOURCE expression context
2. Sets up result number assignment for resjunk attributes to avoid conflicts with target table columns
3. Iterates through each transformed target entry to:
   - Handle resjunk entries by assigning non-conflicting result numbers
   - Resolve column names to attribute numbers in the target relation
   - Validate that specified columns exist in the target table
   - Update target list entries with proper column information and indirection handling
   - Track updated columns for permission checking

The function includes comprehensive error handling for undefined columns and provides helpful hints when users incorrectly qualify column names with relation names in SET clauses.

## Parameters / Member Variables
- : Parse state containing target relation information, namespace items, and permission tracking
- : Original target list from the SET clause containing ResTarget nodes

## Dependencies
- Functions called/Symbols referenced:
  - transformTargetList (standard target list transformation)
  - EXPR_KIND_UPDATE_SOURCE (expression context for UPDATE sources)
  - RelationGetNumberOfAttributes (gets column count from relation)
  - list_head (gets first list cell)
  - attnameAttNum (resolves column name to attribute number)
  - InvalidAttrNumber (invalid attribute constant)
  - updateTargetListEntry (updates target entry with column info)
  - bms_add_member (adds column to permission bitmap)
  - FirstLowInvalidHeapAttributeNumber (heap attribute numbering base)
  - lnext (advances to next list cell)
- Called from (representative examples):
  - transformUpdateStmt (UPDATE statement processing)
  - transformOnConflictClause (INSERT...ON CONFLICT UPDATE processing)  
  - transformMergeStmt (MERGE statement processing)

## Notes and Other Information
This function is central to UPDATE operation processing across multiple statement types including standard UPDATE, MERGE, and INSERT...ON CONFLICT UPDATE. It ensures that resjunk entries (system-generated columns) receive result numbers that don't conflict with actual table columns, which is critical for the rewriter and planner. The permission tracking through updatedCols bitmap is essential for PostgreSQL's security model, ensuring proper column-level UPDATE privileges are enforced. The function provides detailed error reporting with location information and helpful hints for common user mistakes like qualifying SET target columns with relation names.