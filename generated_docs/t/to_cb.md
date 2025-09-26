# to_cb

## Location
[src/test/modules/test_copy_callbacks/test_copy_callbacks.c:25-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_copy_callbacks/test_copy_callbacks.c#L25-L33)

## Overview
A static callback function used in PostgreSQL's test module for COPY TO operations, designed to demonstrate and test custom callback functionality during COPY TO processing.

## Definition

```c
static void
to_cb(void *data, int len)
```
## Detailed Description
The `to_cb` function serves as a simple callback function specifically designed for testing COPY TO callback mechanisms in PostgreSQL. When called during a COPY TO operation, it logs information about the data being processed through PostgreSQL's error reporting system. The function uses `ereport(NOTICE, ...)` to output diagnostic information, making it visible in PostgreSQL's log output or client messages.

This function is part of the test infrastructure located in `src/test/modules/test_copy_callbacks/` and demonstrates how custom callback functions can be integrated with PostgreSQL's COPY TO operations to monitor or process data as it flows through the system.

## Parameters / Member Variables
- `data`: A void pointer to the data buffer being processed during the COPY TO operation, cast to char* for display purposes
- `len`: An integer representing the length of the data buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for logging NOTICE messages)
  - [errmsg](../e/errmsg.md) (for formatting error messages)
- Called from (representative examples):
  - [test_copy_to_callback](test_copy_to_callback.md) (passed as callback function to BeginCopyTo)

## Notes and Other Information
- This function is declared as static, meaning it has internal linkage and is only accessible within the same translation unit
- The function assumes the data buffer contains null-terminated string data when casting to (char *) for display
- The callback mechanism allows for custom processing or monitoring of data during COPY TO operations
- This is primarily used for testing and demonstration purposes rather than production functionality
- The function generates NOTICE-level messages that will be visible to clients connected to the PostgreSQL server