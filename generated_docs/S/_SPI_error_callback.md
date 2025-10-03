# _SPI_error_callback

## Location
[src/backend/executor/spi.c:2961-3006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2961-L3006)

## Overview
 is an error callback function that adds contextual information when a query invoked via SPI fails, providing better error reporting with query details.

## Definition

```c
static void
_SPI_error_callback(void *arg)
```
## Detailed Description
This function serves as an error callback handler within the SPI framework. When a query execution fails, this callback is invoked to enhance the error message with contextual information about the failing query. It handles syntax errors specially by converting them to internal syntax errors with position information, while other errors receive descriptive context based on the query type.

The function uses the SPICallbackArg structure to access the query string and parsing mode, then applies appropriate error context formatting based on whether it's a PL/pgSQL expression, assignment, or regular SQL statement.

## Parameters / Member Variables
- `*arg`: Void pointer to SPICallbackArg structure containing query information and parsing mode
## Dependencies
- Functions called/Symbols referenced:
  - [geterrposition](../g/geterrposition.md): Gets current error position information
  - [errposition](../e/errposition.md): Sets error position to zero (clears external position)
  - [internalerrposition](../i/internalerrposition.md): Sets internal error position for syntax errors
  - [internalerrquery](../i/internalerrquery.md): Sets the query text for internal syntax errors
  - errcontext: Adds contextual information to error messages
- Called from (representative examples):
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md): Cursor opening operations
  - [SPI_plan_get_cached_plan](SPI_plan_get_cached_plan.md): Plan retrieval operations
  - [_SPI_prepare_plan](_SPI_prepare_plan.md): Plan preparation operations
  - [_SPI_prepare_oneshot_plan](_SPI_prepare_oneshot_plan.md): One-shot plan preparation
  - [_SPI_execute_plan](_SPI_execute_plan.md): Plan execution operations

## Notes and Other Information
- Returns early if query string is NULL to handle uninitialized callback arguments
- Distinguishes between syntax errors (with position) and runtime errors
- Provides different context messages for PL/pgSQL expressions vs assignments vs regular SQL
- Uses RAW_PARSE_PLPGSQL_* constants to determine appropriate error context formatting
- Essential for debugging SPI-executed queries by providing meaningful error context

## Simplified Source

```c
static void _SPI_error_callback(void *arg) {
    SPICallbackArg *callback_arg = (SPICallbackArg *) arg;
    const char *query = callback_arg->query;

    // Return early if query not available
    if (query == NULL) {
        return;
    }

    // Handle syntax errors with position information
    int syntax_pos = geterrposition();
    if (syntax_pos > 0) {
        errposition(0);                     // Clear external position
        internalerrposition(syntax_pos);    // Set internal position
        internalerrquery(query);            // Attach query text
    } else {
        // Add context based on query type
        switch (callback_arg->mode) {
            case RAW_PARSE_PLPGSQL_EXPR:
                errcontext("SQL expression \"%s\"", query);
                break;
            case RAW_PARSE_PLPGSQL_ASSIGN1:
            case RAW_PARSE_PLPGSQL_ASSIGN2:
            case RAW_PARSE_PLPGSQL_ASSIGN3:
                errcontext("PL/pgSQL assignment \"%s\"", query);
                break;
            default:
                errcontext("SQL statement \"%s\"", query);
                break;
        }
    }
}
```