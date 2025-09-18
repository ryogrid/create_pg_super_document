# SPI_prepare_params

## Location
src/backend/executor/spi.c: 938 - 975

## Overview
Prepares an SQL statement for execution with custom parameter parsing options and cursor options, returning an execution plan that can be reused for multiple executions.

## Definition
```c
SPIPlanPtr SPI_prepare_params(const char *src,
                              ParserSetupHook parserSetup,
                              void *parserSetupArg,
                              int cursorOptions)
```

## Detailed Description
SPI_prepare_params is an extended version of the basic SPI_prepare function that allows specification of a custom parser setup hook and cursor options. The function parses and prepares an SQL statement for execution, creating a reusable execution plan. Unlike SPI_prepare, this function provides more control over the parsing process through custom hooks and allows cursor-specific options to be set during preparation. The resulting plan can be executed multiple times with SPI_execute_plan or similar functions, making it efficient for repeated execution of the same statement with different parameters.

## Parameters / Member Variables
- `src`: SQL statement string to be prepared
- `parserSetup`: Custom parser setup hook function to be called during parsing (can be NULL)
- `parserSetupArg`: Argument to be passed to the parser setup hook
- `cursorOptions`: Cursor options flags that affect how the plan will be executed

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call
  - [_SPI_prepare_plan](_SPI_prepare_plan.md)
  - _SPI_make_plan_non_temp
  - _SPI_end_call
  - _SPI_PLAN_MAGIC
  - RAW_PARSE_DEFAULT
  - SPI_ERROR_ARGUMENT
- Called from (representative examples):
  - Functions using SPI_OPT_NONATOMIC option

## Notes and Other Information
- Returns NULL on error with SPI_result set to appropriate error code
- The returned plan must be freed with SPI_freeplan when no longer needed
- This function provides more flexibility than SPI_prepare by allowing custom parser hooks
- The plan is copied to the procedure context to ensure it persists beyond the current memory context
- Cursor options specified here will be applied when the plan is executed
- The function initializes a plan structure with no arguments (nargs = 0, argtypes = NULL)