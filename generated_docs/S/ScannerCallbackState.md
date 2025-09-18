# ScannerCallbackState

## Location
[src/include/parser/scanner.h:124-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/scanner.h#L124-L129)

## Overview
A support structure for the scanner_errposition_callback function that provides error context information during SQL parsing and lexical analysis.

## Definition


## Detailed Description
This structure is designed to support error reporting mechanisms in PostgreSQL's scanner. It serves as a callback state container that maintains the necessary context information for generating meaningful error messages with proper position information when lexical analysis encounters problems. The structure works in conjunction with the error callback system to provide detailed diagnostic information about where parsing errors occur within SQL text.

## Parameters / Member Variables
- `yyscanner`: Reference to the core scanner state (core_yyscan_t type)
- `location`: Integer representing the position/location in the input where an error occurred
- `errcallback`: Error context callback structure for managing error reporting chain

## Dependencies
- Functions called/Symbols referenced:
  - core_yyscan_t
- Called from (representative examples):
  - [str_udeescape](../s/str_udeescape.md)

## Notes and Other Information
This structure is primarily used for error handling and diagnostics in the PostgreSQL parser. It enables the scanner to provide precise error location information when reporting parsing problems, which is crucial for debugging SQL statements and providing meaningful error messages to users. The callback mechanism allows for flexible error handling strategies depending on the parsing context.