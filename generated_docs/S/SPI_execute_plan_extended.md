# SPI_execute_plan_extended

## Location
src/backend/executor/spi.c: 711 - 732

## Overview
SPI_execute_plan_extended executes a previously prepared SQL plan with extensible execution options, providing enhanced control over plan execution parameters compared to the basic SPI_execute_plan function.

## Definition
```c
int SPI_execute_plan_extended(SPIPlanPtr plan, const SPIExecuteOptions *options)
```

## Detailed Description
This function serves as an enhanced version of SPI_execute_plan that accepts a comprehensive options structure instead of individual parameters. It executes a previously prepared execution plan with the flexibility to specify various execution parameters through the SPIExecuteOptions structure, including parameter values, read-only mode, row count limits, and other execution-specific settings.

The function provides a more modern and extensible interface for plan execution, allowing for future additions to execution options without changing the function signature. It handles all the standard SPI execution lifecycle including context establishment, plan validation, execution, and cleanup.

## Parameters / Member Variables
- `plan`: Pointer to a previously prepared SPI plan (SPIPlanPtr, must not be NULL and must have valid magic number)
- `options`: Pointer to SPIExecuteOptions structure containing all execution parameters including parameter values, nulls array, read-only flag, row count limit, and other execution options (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call (establishes SPI execution context)
  - [_SPI_execute_plan](_SPI_execute_plan.md) (performs the actual plan execution with options)
  - _SPI_end_call (cleanup SPI execution context)
  - [SPIPlanPtr](SPIPlanPtr.md) (prepared plan type)
  - [SPIExecuteOptions](SPIExecuteOptions.md) (execution options structure)
  - InvalidSnapshot (snapshot constants for execution)
- Called from (representative examples):
  - Referenced in SPI header definitions

## Notes and Other Information
- Returns SPI_ERROR_ARGUMENT if plan is NULL, has invalid magic number, or options is NULL
- This is part of the modern SPI interface that provides more flexible parameter passing
- The options structure approach allows for backward-compatible extensions to execution parameters
- Uses the same internal execution machinery as other SPI execution functions
- Provides a cleaner interface for complex execution scenarios with many options
- The function signature is more future-proof than functions with individual parameters
- Represents the preferred approach for new code requiring plan execution with multiple options