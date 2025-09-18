# parserOpenTable

## Location
[src/backend/parser/parse_relation.c:1418-1469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1418-L1469)

## Overview
Opens a table during parse analysis with enhanced error reporting that includes parse location information for better diagnostics.

## Definition


## Detailed Description
This function is a parser-specific wrapper around table_openrv_extended() that enhances error reporting during query parsing. It sets up error position callbacks to include the RangeVar's parse location in any resulting errors, making diagnostics more helpful. The function provides specialized error messages for different scenarios: qualified vs unqualified relation names, and specifically detects references to forward-declared CTEs that aren't yet in scope, offering targeted hints for resolution.

## Parameters / Member Variables
- : The current parser state containing context information
- : The RangeVar describing the table to open (name, schema, parse location)
- : The lock mode to acquire on the table (typed as int rather than LOCKMODE to avoid header dependencies)

## Dependencies
- Functions called/Symbols referenced:
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md) (error position tracking)
  - table_openrv_extended (actual table opening)
  - [cancel_parser_errposition_callback](../c/cancel_parser_errposition_callback.md) (cleanup error tracking)
  - [isFutureCTE](../i/isFutureCTE.md) (forward CTE reference detection)
  - ereport (error reporting)
  - [ParseCallbackState](../P/ParseCallbackState.md) (error callback state management)
- Called from (representative examples):
  - [setTargetTable](../s/setTargetTable.md)
  - [addRangeTableEntry](../a/addRangeTableEntry.md)
  - (Also exported via parse_relation.h)

## Notes and Other Information
- Lockmode parameter is declared as int rather than LOCKMODE to avoid importing storage/lock.h
- Provides enhanced error messages compared to basic table_openrv()
- Detects and provides specific hints for forward CTE references that cause "relation does not exist" errors
- Uses missing_ok=true when calling table_openrv_extended to handle error reporting internally
- Error position callbacks ensure parse location is included in error messages for better user experience
- Part of the parser infrastructure for robust table access during query analysis