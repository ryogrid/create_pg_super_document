# errorConflictingDefElem

## Location
[src/backend/commands/define.c:384-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/define.c#L384-L390)

## Overview
A utility function that raises a standardized error when conflicting or redundant options are detected in DefElem lists during SQL command processing.

## Definition


## Detailed Description
The `errorConflictingDefElem` function is a centralized error reporting mechanism used throughout PostgreSQL's command processing subsystem to handle cases where duplicate, conflicting, or redundant options are specified in SQL commands. It provides consistent error reporting with proper location information for parser diagnostics.

This function is commonly used during the parsing and validation of various SQL commands that accept option lists (such as CREATE DATABASE, CREATE ROLE, CREATE EXTENSION, COPY, etc.) to ensure that each option appears only once and that conflicting options are not specified together.

The function generates a syntax error with the standard error code `ERRCODE_SYNTAX_ERROR` and includes location information from the problematic DefElem to help users identify the exact position of the conflicting option in their SQL statement.

## Parameters / Member Variables
- `defel`: Pointer to the DefElem structure that represents the conflicting or redundant option. This provides context about which specific option caused the conflict.
- `pstate`: Pointer to the ParseState structure that contains parsing context information, used primarily to determine the location of the error in the original SQL text for accurate error reporting.

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting function)
  - [errcode](errcode.md) (error code specification)
  - [errmsg](errmsg.md) (error message specification)  
  - [parser_errposition](../p/parser_errposition.md) (parser position reporting)
  - [DefElem](../D/DefElem.md) (structure type for definition elements)
  - [ParseState](../P/ParseState.md) (structure type for parse state)
- Called from (representative examples):
  - [createdb](../c/createdb.md) (database creation)
  - [CreateRole](../C/CreateRole.md) (role creation)
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md) (COPY command option processing)
  - [CreateExtension](../C/CreateExtension.md) (extension creation)
  - [DefineCollation](../D/DefineCollation.md) (collation definition)
  - [parse_subscription_options](../p/parse_subscription_options.md) (subscription option parsing)
  - [compute_function_attributes](../c/compute_function_attributes.md) (function attribute computation)
  - [init_params](../i/init_params.md) (sequence parameter initialization)

## Notes and Other Information
- This function never returns - it always throws an ERROR that terminates the current transaction
- The error message "conflicting or redundant options" is standardized across all PostgreSQL commands that use this function
- The function is widely used across the codebase, appearing in over 100 different locations for consistent error handling
- Location information from the DefElem helps provide precise error positioning in SQL statements for better user experience
- This is part of PostgreSQL's centralized approach to error handling and reporting consistency