# SPI_prepare_extended

## Location
[src/backend/executor/spi.c:902-937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L902-L937)

## Overview
SPI_prepare_extended parses and plans a SQL query using an extended options structure that provides fine-grained control over parsing mode, cursor options, and parser setup functions.

## Definition

```c
SPIPlanPtr
SPI_prepare_extended(const char *src,
					 const SPIPrepareOptions *options)
```
## Detailed Description
SPI_prepare_extended is the most flexible preparation function in the SPI interface, accepting a comprehensive options structure rather than individual parameters. It allows callers to specify custom parsing modes, cursor options, and parser setup callbacks, making it suitable for advanced use cases that require non-standard query parsing or execution behavior.

Unlike SPI_prepare and SPI_prepare_cursor which have fixed parameter lists, this function uses the SPIPrepareOptions structure to configure parsing behavior, cursor options, and custom parser setup functions. The function initializes a plan with the provided options and creates a persistent plan suitable for multiple executions.

## Parameters / Member Variables
- `src`: const char * - The SQL query string to prepare
- `options`: const SPIPrepareOptions * - Structure containing preparation options including:
  - `parseMode`: Parse mode for the query (e.g., RAW_PARSE_DEFAULT, RAW_PARSE_PLPGSQL_EXPR)
  - `cursorOptions`: Cursor behavior flags
  - `parserSetup`: Optional parser setup function pointer
  - `parserSetupArg`: Argument to pass to the parser setup function

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_begin_call](_SPI_begin_call.md)
  - [_SPI_prepare_plan](_SPI_prepare_plan.md)
  - [_SPI_make_plan_non_temp](_SPI_make_plan_non_temp.md)
  - [_SPI_end_call](_SPI_end_call.md)
  - [_SPI_plan](_SPI_plan.md)
  - [SPIPlanPtr](SPIPlanPtr.md)
  - [SPIPrepareOptions](SPIPrepareOptions.md)
  - _SPI_PLAN_MAGIC
  - SPI_ERROR_ARGUMENT
- Called from (representative examples):
  - (Referenced in SPI_OPT_NONATOMIC context)

## Notes and Other Information
- Returns SPIPlanPtr on success, NULL on failure (check SPI_result for error details)
- The returned plan must be freed with SPI_freeplan when no longer needed
- Sets SPI_result to indicate success or specific error conditions
- This function does not accept parameter type information directly - parameters must be handled differently or the query must be parameter-free
- The options structure allows for custom parser setup, which is useful for procedural languages with special parsing requirements
- plan.nargs and plan.argtypes are set to 0 and NULL respectively, indicating this function is typically used for parameter-free queries or queries with special parameter handling
- The plan is copied to procedure context to persist beyond the current SPI call
- Most flexible preparation function in the SPI interface, suitable for specialized parsing requirements

## Simplified Source

```c
SPIPlanPtr SPI_prepare_extended(const char *src, const SPIPrepareOptions *options) {
    _SPI_plan plan;
    SPIPlanPtr result;

    // Validate input parameters
    if (src == NULL || options == NULL) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return NULL;
    }

    // Begin SPI execution context
    SPI_result = _SPI_begin_call(true);
    if (SPI_result < 0)
        return NULL;

    // Initialize plan with extended options
    memset(&plan, 0, sizeof(_SPI_plan));
    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = options->parseMode;
    plan.cursor_options = options->cursorOptions;
    plan.nargs = 0;  // No direct parameter support
    plan.argtypes = NULL;
    plan.parserSetup = options->parserSetup;
    plan.parserSetupArg = options->parserSetupArg;

    // Prepare the plan with custom options
    _SPI_prepare_plan(src, &plan);

    // Create persistent plan in procedure context
    result = _SPI_make_plan_non_temp(&plan);

    // Clean up and return result
    _SPI_end_call(true);
    return result;
}
```