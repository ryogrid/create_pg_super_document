# SPI_execp

## Location
src/backend/executor/spi.c: 704 - 710

## Overview
SPI_execp is a legacy wrapper function that provides backward compatibility for executing prepared SPI plans without specifying read-only mode explicitly.

## Definition
```c
int SPI_execp(SPIPlanPtr plan, Datum *Values, const char *Nulls, long tcount)
```

## Detailed Description
This function serves as an obsolete version of SPI_execute_plan, maintained for backward compatibility with older PostgreSQL code. It simply calls SPI_execute_plan with a fixed read_only parameter set to false, meaning all executions through this function are potentially read-write operations.

The function exists primarily to maintain API compatibility for existing code that was written before the read_only parameter was added to the SPI interface. New code should use SPI_execute_plan directly to have explicit control over read-only behavior.

## Parameters / Member Variables
- `plan`: Pointer to a previously prepared SPI plan (SPIPlanPtr, must not be NULL and must have valid magic number)
- `Values`: Array of parameter values as Datum structures (can be NULL if plan has no parameters)
- `Nulls`: Array of null indicators for parameters, one character per parameter (n for null, space or other for not null)
- `tcount`: Maximum number of rows to process (must be non-negative, 0 means no limit)

## Dependencies
- Functions called/Symbols referenced:
  - SPI_execute_plan (delegates all work to this function with read_only=false)
  - SPIPlanPtr (prepared plan type)
- Called from (representative examples):
  - ttdummy (test trigger function)
  - Referenced in SPI header for compatibility

## Notes and Other Information
- This function is marked as obsolete and exists purely for backward compatibility
- Always executes plans in read-write mode (read_only=false), which may not be the desired behavior for all use cases
- New code should use SPI_execute_plan directly to specify read-only behavior explicitly
- The function provides the exact same parameter validation and error handling as SPI_execute_plan
- Part of PostgreSQLs commitment to maintaining backward compatibility across versions
- Essentially a thin wrapper that adds no additional functionality beyond fixing one parameter value