# SPICallbackArg

## Location
[src/backend/executor/spi.c:53-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L53-L57)

## Overview
SPICallbackArg is a structure used in PostgreSQL's Server Programming Interface (SPI) to pass query information and parsing mode data to error callback functions during SQL execution.

## Definition
```c
typedef struct SPICallbackArg
{
    const char *query;
    RawParseMode mode;
} SPICallbackArg;
```

## Detailed Description
SPICallbackArg serves as a container structure that holds context information needed by SPI error callback functions. It encapsulates both the SQL query string and the parsing mode that was being used when an error occurred. This structure is primarily used within the SPI subsystem to provide meaningful error context when SQL operations fail, allowing error handlers to report which query caused the problem and under what parsing conditions.

The structure is designed to be lightweight and contains only the essential information needed for error reporting and debugging within the SPI framework.

## Parameters / Member Variables
- `query`: A constant character pointer containing the SQL query string that was being processed when an error occurred
- `mode`: A RawParseMode enumeration value indicating the parsing mode that was active during query processing

## Dependencies
- Functions called/Symbols referenced:
  - RawParseMode (enumeration type for specifying parse modes)
- Called from (representative examples):
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md)
  - [SPI_plan_get_cached_plan](SPI_plan_get_cached_plan.md)  
  - [_SPI_prepare_plan](_SPI_prepare_plan.md)
  - [_SPI_prepare_oneshot_plan](_SPI_prepare_oneshot_plan.md)
  - [_SPI_execute_plan](_SPI_execute_plan.md)
  - [_SPI_error_callback](_SPI_error_callback.md)

## Notes and Other Information
- This structure is defined as a static typedef in src/backend/executor/spi.c:53-57
- It is primarily used for error handling and debugging purposes within the SPI subsystem
- The structure provides context information to error callback functions to help identify the source of SQL execution failures
- The const qualifier on the query field ensures that the query string cannot be modified through this structure