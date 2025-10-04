# SPI_execute_plan_with_paramlist

## Location
[src/backend/executor/spi.c:733-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L733-L772)

## Overview
SPI_execute_plan_with_paramlist executes a previously prepared SQL plan using a ParamListInfo structure for parameter passing, providing an efficient interface for executing plans with pre-constructed parameter lists.

## Definition
```c
int SPI_execute_plan_with_paramlist(SPIPlanPtr plan, ParamListInfo params,
                                    bool read_only, long tcount)
```

## Detailed Description
This function executes a previously prepared execution plan using a ParamListInfo structure to pass parameters instead of separate Datum and null arrays. ParamListInfo is PostgreSQLs internal structure for managing query parameters, making this function particularly useful when working with parameter lists that have already been constructed in the internal format or when integrating with other parts of the PostgreSQL system that use ParamListInfo.

The function creates an SPIExecuteOptions structure internally and delegates to the same execution machinery used by other SPI execution functions. It provides a more direct interface when parameters are already available in PostgreSQLs internal parameter list format.

## Parameters / Member Variables
- `plan`: Pointer to a previously prepared SPI plan (SPIPlanPtr, must not be NULL and must have valid magic number)
- `params`: ParamListInfo structure containing parameter values and metadata (can be NULL if plan has no parameters)
- `read_only`: Boolean flag indicating whether the query should be executed in read-only mode
- `tcount`: Maximum number of rows to process (must be non-negative, 0 means no limit)

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_begin_call](_SPI_begin_call.md) (establishes SPI execution context)
  - [_SPI_execute_plan](_SPI_execute_plan.md) (performs the actual plan execution with options)
  - [_SPI_end_call](_SPI_end_call.md) (cleanup SPI execution context)
  - [SPIPlanPtr](SPIPlanPtr.md) (prepared plan type)
  - [ParamListInfo](../P/ParamListInfo.md) (parameter list structure type)
  - [SPIExecuteOptions](SPIExecuteOptions.md) (execution options structure)
  - InvalidSnapshot (snapshot constants for execution)
- Called from (representative examples):
  - Referenced in SPI header definitions

## Notes and Other Information
- Returns SPI_ERROR_ARGUMENT if plan is NULL, has invalid magic number, or tcount is negative
- More efficient than SPI_execute_plan when parameters are already in ParamListInfo format
- Avoids the overhead of parameter conversion that SPI_execute_plan requires
- Particularly useful for internal PostgreSQL code that already works with ParamListInfo structures
- The ParamListInfo structure provides more flexibility and metadata compared to simple Datum arrays
- Supports all the same execution features as other SPI plan execution functions
- Part of the modern SPI interface designed for better integration with internal PostgreSQL parameter handling
- Provides a bridge between external SPI usage and internal parameter management systems

## Simplified Source

```c
int SPI_execute_plan_with_paramlist(SPIPlanPtr plan, ParamListInfo params,
                                    bool read_only, long tcount) {
    SPIExecuteOptions options;
    int res;

    // Validate input parameters
    if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC || tcount < 0)
        return SPI_ERROR_ARGUMENT;

    // Begin SPI execution context
    res = _SPI_begin_call(true);
    if (res < 0)
        return res;

    // Set up execution options with provided parameters
    memset(&options, 0, sizeof(options));
    options.params = params;
    options.read_only = read_only;
    options.tcount = tcount;

    // Execute the plan with configured options
    res = _SPI_execute_plan(plan, &options, InvalidSnapshot, InvalidSnapshot, true);

    // Clean up SPI context and return result
    _SPI_end_call(true);
    return res;
}
```