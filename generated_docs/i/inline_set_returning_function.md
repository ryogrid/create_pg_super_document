# inline_set_returning_function

## Location
src/backend/optimizer/util/clauses.c: 5065 - 5357

## Overview
Attempts to inline a set-returning SQL function in the FROM clause by expanding the function body and returning a substitute Query structure, enabling optimization of set-returning function calls.

## Definition


## Detailed Description
This function performs inline expansion of set-returning SQL functions that appear in range table entries (FROM clause). It analyzes whether a given function call can be safely inlined and, if so, parses and processes the function body to create a substitute Query structure that replaces the function call. The inlining process involves extensive validation to ensure the substitution is safe and semantically equivalent to the original function call.

The function performs several critical safety checks: verifying that the function is SQL-language, not strict, not volatile, doesn't have security definer properties, and returns a set. It also ensures that arguments don't contain volatile functions or subplans that could change behavior when evaluated multiple times. The function handles both traditional prosrc-based function definitions and newer prosqlbody-based definitions.

The inlining process involves parsing the function body, analyzing and rewriting it with proper parameter substitution, and validating that the result type matches the declared function signature. Special attention is given to composite return types and tuple result validation. The function creates appropriate memory contexts for temporary allocations and sets up error callbacks to provide meaningful error messages during the inlining process.

## Parameters / Member Variables
- : PlannerInfo containing global information about the query being planned
- : RangeTblEntry representing the function call in the FROM clause (must be RTE_FUNCTION type)

## Dependencies
- Functions called/Symbols referenced:
  -  - prevents infinite recursion in self-referential functions
  -  - checks for volatile functions in arguments
  -  - checks for subplans in arguments
  -  - verifies execute permissions on the function
  -  - checks if function has entry/exit hooks
  -  - checks for NULL attributes in pg_proc tuple
  -  - sets up parameter information for parsing
  -  - parses the function body SQL
  -  - analyzes and rewrites the parsed query
  -  - configures parser hooks for SQL functions
  -  - validates function return type compatibility
  -  - replaces parameters with actual arguments
  -  - records plan dependency on the function
  -  - [error](../e/error.md) callback for enhanced error reporting

- Called from (representative examples):
  -  - during query preprocessing to inline eligible functions

## Notes and Other Information
- Only processes RTE_FUNCTION entries that represent single, simple FuncExpr nodes
- Fails for functions with ORDINALITY (WITH ORDINALITY clause)
- Requires functions to be SQL-language, not strict, not volatile, and declared as set-returning
- Creates temporary memory contexts to avoid memory leaks during parsing and processing
- Handles both prosrc (traditional) and prosqlbody (newer) function body storage formats
- Performs extensive type checking to ensure inlined query matches declared function signature
- For composite return types, validates that the function returns complete tuples rather than individual composite values
- Records dependencies and row-level security requirements from the inlined query
- Returns NULL if inlining is not possible or safe, allowing fallback to regular function execution
- The inlined query replaces the original function call, potentially enabling further optimizations by the planner