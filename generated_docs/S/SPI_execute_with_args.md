# SPI_execute_with_args

## Location
[src/backend/executor/spi.c:812-859](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L812-L859)

## Overview
SPI_execute_with_args plans and executes a query with supplied arguments in a single operation, providing a convenient alternative to separate SPI_prepare and SPI_execute_plan calls.

## Definition

```c
int
SPI_execute_with_args(const char *src,
					  int nargs, Oid *argtypes,
					  Datum *Values, const char *Nulls,
					  bool read_only, long tcount)
```
## Detailed Description
SPI_execute_with_args is functionally equivalent to calling SPI_prepare followed by SPI_execute_plan, but performs both operations in a single function call. It creates a temporary plan structure, prepares a one-shot plan from the provided SQL source, converts the parameters, and executes the plan. This function is useful for executing parameterized queries that don't need to be reused, avoiding the overhead of managing separate prepare and execute phases.

The function uses InvalidSnapshot for both snapshot parameters and fires triggers immediately (fire_triggers=true), making it suitable for standard query execution scenarios.

## Parameters / Member Variables
- `src`: const char * - The SQL query string to execute
- `nargs`: int - Number of arguments expected by the query
- `argtypes`: Oid * - Array of PostgreSQL type OIDs for the arguments
- `Values`: Datum * - Array of parameter values for the query
- `Nulls`: const char * - Array indicating which parameters are NULL
- `read_only`: bool - Whether the execution should be read-only
- `tcount`: long - Maximum number of tuples to process

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call
  - [_SPI_convert_params](_SPI_convert_params.md)
  - [_SPI_prepare_oneshot_plan](_SPI_prepare_oneshot_plan.md)
  - [_SPI_execute_plan](_SPI_execute_plan.md)
  - _SPI_end_call
  - _SPI_plan
  - [ParamListInfo](../P/ParamListInfo.md)
  - [SPIExecuteOptions](SPIExecuteOptions.md)
  - _SPI_PLAN_MAGIC
  - RAW_PARSE_DEFAULT
  - CURSOR_OPT_PARALLEL_OK
  - InvalidSnapshot
  - SPI_ERROR_ARGUMENT
  - SPI_ERROR_PARAM
- Called from (representative examples):
  - (Referenced in SPI_OPT_NONATOMIC context)

## Notes and Other Information
- This function is ideal for one-time query execution with parameters
- Automatically handles plan creation and cleanup internally
- Uses default parsing mode (RAW_PARSE_DEFAULT) and allows parallel execution (CURSOR_OPT_PARALLEL_OK)
- Returns standard SPI result codes (SPI_OK_*, SPI_ERROR_*)
- Validates that argument arrays are provided when nargs > 0
- The created plan is temporary and not reusable - use SPI_prepare for reusable plans
- Uses InvalidSnapshot for both regular and crosscheck snapshots, resulting in default snapshot behavior