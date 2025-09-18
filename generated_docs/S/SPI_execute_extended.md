# SPI_execute_extended

## Location
[src/backend/executor/spi.c:637-671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L637-L671)

## Overview
SPI_execute_extended is an enhanced version of the SPI_execute function that provides extensible execution options for parsing, planning, and executing SQL query strings from within server-side functions.

## Definition
```c
int SPI_execute_extended(const char *src, const SPIExecuteOptions *options)
```

## Detailed Description
This function serves as an advanced interface to the PostgreSQL Server Programming Interface (SPI) for executing SQL queries. Unlike the basic SPI_execute function, SPI_execute_extended accepts an options structure that allows for more flexible control over query execution parameters including custom parser setups, parameter handling, and execution modes.

The function creates a temporary execution plan, executes it with the provided options, and then cleans up. It handles the complete lifecycle of query execution including parsing, planning, and execution phases within a single call.

## Parameters / Member Variables
- `src`: The SQL query string to be executed (must not be NULL)
- `options`: Pointer to SPIExecuteOptions structure containing execution parameters including parameter setup callbacks and execution options (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call (initializes SPI execution context)
  - [_SPI_prepare_oneshot_plan](_SPI_prepare_oneshot_plan.md) (parses and plans the query)
  - [_SPI_execute_plan](_SPI_execute_plan.md) (executes the prepared plan)
  - _SPI_end_call (cleans up SPI execution context)
  - [SPIExecuteOptions](SPIExecuteOptions.md) (options structure type)
  - _SPI_plan (internal plan structure)
- Called from (representative examples):
  - Referenced in SPI header definitions

## Notes and Other Information
- Returns SPI_ERROR_ARGUMENT if either src or options parameters are NULL
- The function uses a "oneshot" planning approach, meaning the plan is created, executed, and discarded in a single operation
- Supports parallel execution through CURSOR_OPT_PARALLEL_OK cursor option
- Uses RAW_PARSE_DEFAULT parsing mode for standard SQL parsing
- All execution happens within a proper SPI call context established by _SPI_begin_call/_SPI_end_call
- The function is part of the PostgreSQL 9.5+ SPI interface enhancements that provide more granular control over query execution