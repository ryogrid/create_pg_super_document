# transformLimitClause

## Location
src/backend/parser/parse_clause.c: 1881 - 1924

## Overview
Transforms SQL LIMIT and OFFSET clause expressions into internal expression trees, ensuring they are of type bigint and meet semantic requirements.

## Definition


## Detailed Description
This function is responsible for processing LIMIT and OFFSET clauses in SQL SELECT statements and related constructs. It performs several critical transformations and validations: first, it calls transformExpr to convert the raw clause into a proper expression tree, then coerces the result to INT8 (bigint) type as required by PostgreSQL's LIMIT implementation since version 8.2. The function also enforces that LIMIT expressions cannot reference variables from the current query level by calling checkExprIsVarFree. Additionally, it includes special validation for FETCH FIRST ... WITH TIES constructs to prevent NULL values, which would cause issues in query rule generation.

## Parameters / Member Variables
- : The current parsing state containing context information for the transformation
- : The raw parse tree node representing the LIMIT/OFFSET expression to be transformed  
- : An enumeration value specifying the expression context (EXPR_KIND_LIMIT, etc.)
- : A descriptive string used in error messages to identify the SQL construct
- : An enumeration specifying the type of limit option (WITH TIES, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - transformExpr (expression transformation)
  - coerce_to_specific_type (type coercion to INT8)
  - checkExprIsVarFree (variable reference validation)
  - ParseExprKind (enumeration type)
  - LimitOption (enumeration type)
  - A_Const (constant node type)
  - LIMIT_OPTION_WITH_TIES (enum value)
  - EXPR_KIND_LIMIT (enum value)
- Called from (representative examples):
  - transformSelectStmt
  - transformValuesClause
  - transformSetOperationStmt
  - transformPLAssignStmt

## Notes and Other Information
- Returns NULL if the input clause is NULL, making LIMIT/OFFSET clauses optional
- Since PostgreSQL 8.2, LIMIT expressions must be INT8 (bigint) rather than INT4 (integer)
- The function prevents variable references in LIMIT clauses to ensure they are constant expressions
- Special validation exists for FETCH FIRST ... WITH TIES to prevent NULL literals that would break query rule generation
- The constructName parameter is used purely for error reporting
- This function is declared in parse_clause.h and used throughout the parser for various SELECT-related constructs
- The checkExprIsVarFree call ensures LIMIT values are deterministic and don't depend on query results