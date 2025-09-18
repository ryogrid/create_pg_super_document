# check_srf_call_placement

## Location
src/backend/parser/parse_func.c: 2511 - 2682

## Overview
check_srf_call_placement validates that set-returning functions (SRFs) are called in syntactically valid locations within SQL queries and sets appropriate parser state flags for query planning.

## Definition
```c
void check_srf_call_placement(ParseState *pstate, Node *last_srf, int location)
```

## Detailed Description
This function enforces PostgreSQL's restrictions on where set-returning functions can be used within SQL statements. It examines the current parsing context (stored in pstate->p_expr_kind) to determine if an SRF call is valid in the current location. Valid locations include SELECT target lists, GROUP BY/ORDER BY expressions, window partition/order clauses, and function calls in FROM clauses. The function prevents SRFs from being used in inappropriate contexts like WHERE clauses, JOIN conditions, CHECK constraints, and DEFAULT expressions. When SRFs are found in valid target contexts, it sets pstate->p_hasTargetSRFs to true, which affects query planning decisions. For FROM function contexts, it additionally checks for nested SRFs which are not supported.

## Parameters / Member Variables
- `pstate`: ParseState containing current parsing context and expression kind information
- `last_srf`: Snapshot of pstate->p_last_srf from before parsing function arguments, used to detect nested SRFs
- `location`: Source location of the SRF call for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errmsg_internal](../e/errmsg_internal.md)
  - [parser_errposition](../p/parser_errposition.md)
  - [exprLocation](../e/exprLocation.md)
  - [ParseExprKindName](../P/ParseExprKindName.md)
  - ERRCODE_FEATURE_NOT_SUPPORTED
  - Various EXPR_KIND_* constants
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - Function call transformation code
  - Expression parsing functions

## Notes and Other Information
The function uses a comprehensive switch statement covering all possible ParseExprKind values to ensure complete coverage as new expression contexts are added to PostgreSQL. It employs two error reporting mechanisms: custom error messages for complex contexts and generic messages using ParseExprKindName for simple SQL keyword contexts. The p_hasTargetSRFs flag set by this function is crucial for query planning as it indicates that the query will produce multiple result rows per input row. The nested SRF detection prevents complex cases that would be difficult to handle during execution, requiring SRFs in FROM clauses to appear at the top level only.