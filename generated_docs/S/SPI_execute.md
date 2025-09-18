# SPI_execute

## Location
src/backend/executor/spi.c: 596 - 629

## Overview
SPI_execute parses, plans, and executes a SQL query string, providing a high-level interface for executing arbitrary SQL from within PostgreSQL server code.

## Definition
```c
int SPI_execute(const char *src, bool read_only, long tcount)
```

## Detailed Description
This is one of the primary SPI functions for executing SQL queries. It takes a SQL query string and handles the complete execution cycle: parsing, planning, and execution. The function creates a temporary execution plan that is used only for this single execution ("oneshot" plan).

The function performs input validation, begins an SPI call context, prepares a oneshot plan with the provided SQL string, sets up execution options, executes the plan, and cleans up the call context. It supports both read-only and read-write operations based on the read_only parameter.

The execution uses default parsing mode (RAW_PARSE_DEFAULT) and enables parallel query execution where possible (CURSOR_OPT_PARALLEL_OK).

## Parameters / Member Variables
- `src`: The SQL query string to execute (must not be NULL)
- `read_only`: Boolean flag indicating whether this is a read-only query
- `tcount`: Maximum number of rows to process, or 0 for no limit (must be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_plan (struct type for execution plans)
  - [SPIExecuteOptions](SPIExecuteOptions.md) (struct type for execution options)
  - SPI_ERROR_ARGUMENT (error code constant)
  - _SPI_begin_call (function to start SPI call context)
  - _SPI_PLAN_MAGIC (magic number for plan validation)
  - RAW_PARSE_DEFAULT (parsing mode constant)
  - CURSOR_OPT_PARALLEL_OK (cursor option constant)
  - [_SPI_prepare_oneshot_plan](_SPI_prepare_oneshot_plan.md) (function to prepare temporary plan)
  - [_SPI_execute_plan](_SPI_execute_plan.md) (function to execute prepared plan)
  - InvalidSnapshot (snapshot constant)
  - _SPI_end_call (function to end SPI call context)

- Called from (representative examples):
  - [refresh_by_match_merge](../r/refresh_by_match_merge.md) (src/backend/commands/matview.c:653)
  - [SPI_exec](SPI_exec.md) (src/backend/executor/spi.c:632)
  - [query_to_oid_list](../q/query_to_oid_list.md) (src/backend/utils/adt/xml.c:2792)
  - [plperl_spi_exec](../p/plperl_spi_exec.md) (src/pl/plperl/plperl.c:3156)
  - [PLy_spi_execute_query](../P/PLy_spi_execute_query.md) (src/pl/plpython/plpy_spi.c:315)

## Notes and Other Information
- Higher-level alternative to SPI_execute_plan for simple query execution
- Creates and destroys execution plan internally (oneshot plan)
- Widely used by procedural languages and internal PostgreSQL code
- Returns standard SPI result codes (SPI_OK_*, SPI_ERROR_*)
- Thread-safe through SPI call context management
- Located in src/backend/executor/spi.c:596-629
- Part of PostgreSQL's Server Programming Interface (SPI)