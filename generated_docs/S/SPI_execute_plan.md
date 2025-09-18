# SPI_execute_plan

## Location
[src/backend/executor/spi.c:672-703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L672-L703)

## Overview
SPI_execute_plan executes a previously prepared SQL plan with parameter values, providing a fundamental mechanism for executing parameterized queries through the Server Programming Interface.

## Definition
```c
int SPI_execute_plan(SPIPlanPtr plan, Datum *Values, const char *Nulls,
                     bool read_only, long tcount)
```

## Detailed Description
This function executes a previously prepared execution plan (created by SPI_prepare or similar functions) with the provided parameter values. It serves as the core execution interface for prepared statements in PostgreSQLs SPI framework. The function validates the plan, converts the provided parameter values into the internal format expected by the executor, and then delegates to the internal execution machinery.

The function handles parameter validation, type conversion, and ensures proper SPI execution context management. It supports both read-only and read-write execution modes and allows limiting the number of rows processed.

## Parameters / Member Variables
- `plan`: Pointer to a previously prepared SPI plan (SPIPlanPtr, must not be NULL and must have valid magic number)
- `Values`: Array of parameter values as Datum structures (can be NULL if plan has no parameters)
- `Nulls`: Array of null indicators for parameters, one character per parameter (n for null, space or other for not null)
- `read_only`: Boolean flag indicating whether the query should be executed in read-only mode
- `tcount`: Maximum number of rows to process (must be non-negative, 0 means no limit)

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call (establishes SPI execution context)
  - [_SPI_convert_params](_SPI_convert_params.md) (converts parameter values to internal format)
  - [_SPI_execute_plan](_SPI_execute_plan.md) (performs the actual plan execution)
  - _SPI_end_call (cleanup SPI execution context)
  - [SPIPlanPtr](SPIPlanPtr.md) (prepared plan type)
  - [SPIExecuteOptions](SPIExecuteOptions.md) (execution options structure)
- Called from (representative examples):
  - [SPI_execp](SPI_execp.md) (wrapper function)
  - [pg_get_ruledef_worker](../p/pg_get_ruledef_worker.md) (rule definition retrieval)
  - [pg_get_viewdef_worker](../p/pg_get_viewdef_worker.md) (view definition retrieval)
  - [plperl_spi_exec_prepared](../p/plperl_spi_exec_prepared.md) (PL/Perl prepared statement execution)
  - [PLy_spi_execute_plan](../P/PLy_spi_execute_plan.md) (PL/Python plan execution)

## Notes and Other Information
- Returns SPI_ERROR_ARGUMENT if plan is NULL, has invalid magic number, or tcount is negative
- Returns SPI_ERROR_PARAM if the plan expects parameters but Values is NULL
- The function is widely used across PostgreSQL procedural languages (PL/Perl, PL/Python, PL/Tcl) for executing prepared statements
- Parameter conversion handles the mapping between external parameter representations and internal Datum format
- The read_only flag provides an additional safety mechanism for preventing modifications in read-only contexts
- This is one of the most commonly used SPI functions for executing parameterized queries