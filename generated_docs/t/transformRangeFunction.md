# transformRangeFunction

## Location
src/backend/parser/parse_clause.c: 465 - 687

## Overview
Transforms function calls appearing in FROM clauses into ParseNamespaceItems, handling special cases like multi-argument UNNEST() expansion and column definition lists.

## Definition
static ParseNamespaceItem *
transformRangeFunction(ParseState *pstate, RangeFunction *r)

## Detailed Description
The transformRangeFunction function handles the complex transformation of function calls that appear in FROM clauses. This function manages several intricate aspects: it automatically enables lateral references for all function calls (required for SQL spec compliance with UNNEST), handles the special case of multi-argument UNNEST() by expanding it into separate single-argument calls, processes column definition lists with proper validation, ensures set-returning functions appear at the top level, and manages collation assignment. The function also determines whether the RTE should be marked as LATERAL based on explicit specification or cross-references detection.

## Parameters / Member Variables
- pstate: ParseState structure containing the current parsing context and state information
- r: RangeFunction structure representing the function call(s) to be transformed, including function expressions, column definitions, lateral flag, and ordinality specification

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md)
  - [FigureColname](../F/FigureColname.md)
  - makeFuncCall
  - SystemFuncName
  - [assign_list_collations](../a/assign_list_collations.md)
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)
  - EXPR_KIND_FROM_FUNCTION
  - COERCE_EXPLICIT_CALL
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- The function automatically sets p_lateral_active = true for all function calls in FROM, regardless of explicit LATERAL marking
- Special handling for UNNEST() with multiple arguments: transforms unnest(a,b,c) into separate unnest(a), unnest(b), unnest(c) calls using pg_catalog.unnest
- Validates that set-returning functions appear at the top level of FROM clauses, enforcing nodeFunctionscan.c requirements
- Column definition lists are validated to prevent conflicts between per-function and top-level definitions
- Restrictions on column definition lists: only allowed with single functions and not with WITH ORDINALITY
- The function determines LATERAL marking based on either explicit specification (r->lateral) or presence of lateral cross-references
- Collation assignment is performed before creating the RTE to ensure proper collation information is available for Vars
- Error handling includes specific messages for ROWS FROM() vs UNNEST() syntax violations