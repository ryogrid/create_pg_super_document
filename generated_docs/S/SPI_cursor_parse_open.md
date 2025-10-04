# SPI_cursor_parse_open

## Location
[src/backend/executor/spi.c:1533-1576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1533-L1576)

## Overview
SPI_cursor_parse_open is a modern, flexible interface for parsing a SQL query and opening it as a cursor, using a structured options parameter to provide comprehensive control over parsing and cursor behavior.

## Definition
```c
Portal SPI_cursor_parse_open(const char *name, const char *src, const SPIParseOpenOptions *options)
```

## Detailed Description
This function represents the most flexible and modern approach to combining SQL parsing and cursor opening in a single operation. It uses the SPIParseOpenOptions structure to encapsulate all configuration options, providing a clean, extensible interface that can accommodate future enhancements without changing the function signature.

The function performs these key operations:
1. **Input Validation**: Ensures required parameters (src and options) are provided
2. **SPI Context Management**: Properly initializes and cleans up the SPI execution context
3. **Plan Initialization**: Creates a temporary plan structure with appropriate defaults
4. **Option Processing**: Applies cursor options and parser setup from the options structure
5. **Query Parsing**: Parses and plans the SQL query string
6. **Cursor Creation**: Opens the prepared query as a cursor with specified parameters
7. **Resource Cleanup**: Ensures proper cleanup of SPI context

This design pattern follows modern PostgreSQL practices of using option structures for complex function interfaces, making the API more maintainable and extensible.

## Parameters / Member Variables
- `name`: Name to assign to the portal/cursor. Can be NULL for an unnamed portal.
- `src`: SQL query string to be parsed and executed. Must not be NULL.
- `options`: Pointer to SPIParseOpenOptions structure containing:
  - `cursorOptions`: Integer bitmask of cursor options (scrollable, holdable, etc.)
  - `params`: ParamListInfo for query parameters (can be NULL)
  - `read_only`: Boolean indicating if cursor should be read-only

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_begin_call](_SPI_begin_call.md) (initialize SPI context)
  - [_SPI_prepare_plan](_SPI_prepare_plan.md) (parse and plan the query)
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md) (create the actual cursor)
  - [_SPI_end_call](_SPI_end_call.md) (cleanup SPI context)
  - elog (error logging)
- Called from (representative examples):
  - Available through SPI interface for modern applications
  - Used by code that needs fine-grained control over parsing and cursor options

## Notes and Other Information
- This function represents the most modern and flexible approach to SPI cursor creation.
- The SPIParseOpenOptions structure allows for future extension without breaking API compatibility.
- Supports advanced features like custom parser setup functions through the options structure.
- Provides the cleanest separation between different configuration aspects (cursor options, parameters, read-only flag).
- Recommended for new code that needs comprehensive control over cursor creation and parsing.
- The structured options approach makes the function more maintainable and self-documenting than parameter-heavy alternatives.
- Combines the convenience of SPI_cursor_open_with_args with the flexibility of ParamListInfo-based parameter passing.

## Simplified Source

```c
Portal SPI_cursor_parse_open(const char *name, const char *src,
                            const SPIParseOpenOptions *options) {
    Portal result;
    _SPI_plan plan;

    // Validate arguments
    if (src == NULL || options == NULL)
        elog(ERROR, "SPI_cursor_parse_open called with invalid arguments");

    // Begin SPI operation
    SPI_result = _SPI_begin_call(true);
    if (SPI_result < 0)
        elog(ERROR, "SPI_cursor_parse_open called while not connected");

    // Initialize plan with options
    memset(&plan, 0, sizeof(_SPI_plan));
    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = options->cursorOptions;
    if (options->params) {
        plan.parserSetup = options->params->parserSetup;
        plan.parserSetupArg = options->params->parserSetupArg;
    }

    // Parse and prepare the query
    _SPI_prepare_plan(src, &plan);

    // Create the cursor
    result = SPI_cursor_open_internal(name, &plan, options->params, options->read_only);

    // Clean up
    _SPI_end_call(true);

    return result;
}
```