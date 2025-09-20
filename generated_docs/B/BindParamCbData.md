# BindParamCbData

## Location
[src/backend/tcop/postgres.c:116-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L116-L121)

## Overview
A callback data structure used for error reporting during parameter binding in the Bind message processing phase of the PostgreSQL frontend/backend protocol.

## Definition

```c
typedef struct BindParamCbData
{
	const char *portalName;
	int			paramno;		/* zero-based param number, or -1 initially */
	const char *paramval;		/* textual input string, if available */
} BindParamCbData;
```
## Detailed Description
 is a specialized data structure designed to provide contextual information for error reporting during parameter binding operations. It serves as the argument to the  function, which is registered as an error context callback during the processing of Bind protocol messages. This structure enables PostgreSQL to provide meaningful error messages that include the portal name, parameter number, and parameter value when parameter binding fails.

The structure is used within the  function where it's initialized once and then updated for each parameter as they are processed sequentially. This approach allows for efficient error reporting without the overhead of maintaining separate error context for each parameter.

## Parameters / Member Variables
- : The name of the portal being bound, used to identify which prepared statement is involved in the error
- : Zero-based parameter number currently being processed, or -1 when not processing any specific parameter
- : Textual representation of the parameter value being processed, used for including the actual value in error messages when available

## Dependencies
- Functions called/Symbols referenced: None (this is a pure data structure)
- Called from (representative examples):
  -  (initializes and maintains the structure during parameter binding)
  -  (consumes the structure to generate contextual error messages)

## Notes and Other Information
- This structure is typically allocated on the stack within  and passed by reference to the error callback system
- The  field is initialized to -1 and updated to the current parameter index during processing
- The  field may be NULL if no textual representation is available
- The error callback uses this information to generate user-friendly error messages that include portal name, parameter number (1-based in messages), and parameter value when reporting binding failures
- This design pattern demonstrates PostgreSQL's approach to providing detailed error context without impacting normal execution performance