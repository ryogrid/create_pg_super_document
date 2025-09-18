# transformDeclareCursorStmt

## Location
src/backend/parser/analyze.c: 2868 - 2960

## Overview
Transforms a DECLARE CURSOR statement into a CMD_UTILITY Query node after validating cursor options and transforming the contained SELECT query.

## Definition


## Detailed Description
This function processes a DECLARE CURSOR statement by first validating the compatibility of various cursor options, then transforming the contained query (which must be a SELECT), and finally wrapping the result as a utility statement. The transformation occurs during parse analysis rather than execution to ensure parser hooks are triggered at the expected time.

The function performs several validation checks:
1. Ensures conflicting cursor options are not specified together (SCROLL/NO SCROLL, ASENSITIVE/INSENSITIVE)
2. Verifies the contained statement is a SELECT query
3. Prohibits data-modifying WITH clauses in cursors
4. Checks compatibility between FOR UPDATE clauses and cursor options (HOLD, SCROLL, INSENSITIVE)

After validation, it transforms the contained query and packages the entire statement as a CMD_UTILITY query for execution.

## Parameters / Member Variables
- : Parse state containing context information for the transformation
- : The DECLARE CURSOR statement to transform, containing:
  - : Cursor options flags (SCROLL, NO SCROLL, HOLD, etc.)
  - : The SELECT statement defining the cursor
  - : Name of the cursor (stored in stmt)

## Dependencies
- Functions called/Symbols referenced:
  - [transformStmt](transformStmt.md), makeNode
  - ereport, errcode, errmsg, errdetail
  - [LCS_asString](../L/LCS_asString.md), linitial
  - IsA macro for type checking
- Constants referenced:
  - CURSOR_OPT_SCROLL, CURSOR_OPT_NO_SCROLL
  - CURSOR_OPT_ASENSITIVE, CURSOR_OPT_INSENSITIVE 
  - CURSOR_OPT_HOLD, CMD_SELECT, CMD_UTILITY
  - Error codes: ERRCODE_INVALID_CURSOR_DEFINITION, ERRCODE_FEATURE_NOT_SUPPORTED
- Called from (representative examples):
  - [transformStmt](transformStmt.md)

## Notes and Other Information
- The function enforces SQL standard restrictions on cursor options compatibility
- Holdable cursors, scrollable cursors, and insensitive cursors must all be READ ONLY
- Data-modifying CTEs are not allowed in cursor definitions for semantic clarity
- The transformed query is stored back in the original statement structure
- Returns a CMD_UTILITY query rather than executing the cursor declaration directly
- Parser hook side effects occur during this transformation phase, not at execution time