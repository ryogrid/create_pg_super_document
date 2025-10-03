# debugStartup

## Location
[src/backend/access/common/printtup.c:444-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L444-L461)

## Overview
The debugStartup function prepares for debug output by displaying the schema information of tuples that will be processed by an interactive backend.

## Definition

```c
void
debugStartup(DestReceiver *self, int operation, TupleDesc typeinfo)
```
## Detailed Description
The debugStartup function is a debugging utility that displays the structure of tuples before they are processed. It serves as a startup handler for DestReceiver operations in debug mode, providing developers with insight into the tuple descriptor that will be used for subsequent tuple processing.

The function iterates through all attributes in the provided tuple descriptor and calls printatt for each one to display detailed information about each attribute including its name, type, length, and other metadata. After displaying all attributes, it prints a separator line to clearly delineate the schema information from subsequent tuple data.

This function is typically used in development and debugging scenarios where developers need to understand the structure of data being processed by PostgreSQL's tuple handling mechanisms.

## Parameters / Member Variables
- `*self`: DestReceiver pointer (not used in this function but required for interface compatibility)
- `operation`: Integer representing the type of operation being performed (not used in this function)
- `typeinfo`: TupleDesc structure containing the schema information for the tuples to be processed
## Dependencies
- Functions called/Symbols referenced:
  - [printatt](../p/printatt.md): Utility function to print detailed attribute information
  - TupleDescAttr: Macro to access attribute information from tuple descriptor
  - printf: Standard C library function for formatted output
- Called from (representative examples):
  - [donothingCleanup](donothingCleanup.md): Referenced as a startup handler for debugging operations
  - PRINTTUP_H: Declared in the printtup.h header file for external use

## Notes and Other Information
- Unlike other functions in printtup.c, this function is not marked as static, making it available for use in other modules
- The function is declared in src/include/access/printtup.h for external access
- The function parameters 'self' and 'operation' are not used but are required to match the DestReceiver startup function signature
- The separator line ('----') helps visually distinguish schema information from actual tuple data in debug output
- Attribute numbering starts from 1 (i+1) to match PostgreSQL's convention where attribute numbers are 1-based
- This is primarily a development tool and not intended for production use