# ParseCallbackState

## Location
[src/include/parser/parse_node.h:332-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/parse_node.h#L332-L337)

## Overview
ParseCallbackState is a support structure for providing parser error position information in PostgreSQL's error reporting system, enabling accurate error location reporting during SQL parsing operations.

## Definition

```c
typedef struct ParseCallbackState
{
	ParseState *pstate;
	int			location;
	ErrorContextCallback errcallback;
} ParseCallbackState;
```
## Detailed Description
ParseCallbackState serves as a callback mechanism for the PostgreSQL error reporting system to provide accurate source location information when parsing errors occur. This structure is used to set up an error context callback that can insert parser error position information into error messages.

The structure works by registering itself with PostgreSQL's error context stack via setup_parser_errposition_callback(). When an error occurs anywhere in the code while this callback is active, the pcb_error_callback function is invoked, which extracts the stored parse state and location to provide meaningful error position information to the user.

The callback is designed to be non-intrusive and only adds location information for relevant errors (excluding query cancellation errors). It integrates with PostgreSQL's ereport() error reporting system to enhance error messages with precise source code positions.

## Parameters / Member Variables
- `*pstate`: Pointer to the current ParseState containing parsing context and information
- `location`: Character position in the source query where the error occurred
- `errcallback`: ErrorContextCallback structure for integration with PostgreSQL's error handling system
## Dependencies
- Functions called/Symbols referenced:
  - [ParseState](ParseState.md) (from parser subsystem)
  - ErrorContextCallback (from PostgreSQL error handling system)
- Called from (representative examples):
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md) (src/backend/parser/parse_node.c:140)
  - [cancel_parser_errposition_callback](../c/cancel_parser_errposition_callback.md) (src/backend/parser/parse_node.c:156)
  - [coerce_type](../c/coerce_type.md) (src/backend/parser/parse_coerce.c:257)
  - [transformSetOperationTree](../t/transformSetOperationTree.md) (src/backend/parser/analyze.c:2288)

## Notes and Other Information
- Part of PostgreSQL's comprehensive error reporting infrastructure for parser operations
- Provides stack-based error context management, allowing nested callback registration
- The callback automatically filters out irrelevant errors like query cancellations
- Essential for providing user-friendly error messages with accurate source positions
- Used throughout the parser subsystem wherever precise error location reporting is needed
- Must be properly paired with setup and cancel calls to maintain error context stack integrity