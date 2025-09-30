# sql_inline_error_callback

## Location
[src/backend/optimizer/util/clauses.c:4949-4972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4949-L4972)

## Overview
An error context callback function that provides call-stack traceback information during SQL function inlining errors, converting syntax errors to internal error reports with proper context.

## Definition

```c
static void
sql_inline_error_callback(void *arg)
```
## Detailed Description
This function serves as an error callback handler specifically designed for SQL function inlining operations. When an error occurs during the inlining process, this callback is invoked to provide enhanced error reporting with proper context information. The function performs two main tasks: handling syntax errors by converting external syntax error positions to internal ones, and adding contextual information about which SQL function was being processed when the error occurred.

The callback checks if the current error is a syntax error by examining the error position. If a syntax error is detected (position > 0), it clears the external error position, converts it to an internal error position, and associates it with the original function source code. This conversion is crucial for providing accurate error location information to the user, as the error position needs to be mapped back to the original function definition rather than the inlined version.

## Parameters / Member Variables
- : A void pointer that points to an  structure containing callback context information including the function name () and source code ()

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type)
  -  - retrieves current syntax error position
  -  - sets/clears external error position
  -  - sets internal error position
  -  - associates error with internal query text
  -  - adds contextual error message

- Called from (representative examples):
  -  - during regular SQL function inlining
  -  - during set-returning function inlining
  -  - during parallel hazard analysis

## Notes and Other Information
- This is a static function local to , used exclusively for error handling during inlining operations
- The callback mechanism allows PostgreSQL's error system to provide meaningful stack traces even when errors occur in dynamically processed code
- The conversion from external to internal error positions ensures that error messages point to the correct location in the original function source rather than processed/inlined code
- The function is typically registered as an error callback before attempting inlining operations and unregistered afterward

## Simplified Source

```c
static void
sql_inline_error_callback(void *arg)
{
    inline_error_callback_arg *callback_arg = (inline_error_callback_arg *) arg;
    int syntaxerrposition;

    // Check if this is a syntax error and get its position
    syntaxerrposition = geterrposition();
    if (syntaxerrposition > 0)
    {
        // Convert external syntax error to internal error format
        errposition(0);                                    // Clear external position
        internalerrposition(syntaxerrposition);           // Set internal position
        internalerrquery(callback_arg->prosrc);           // Associate with function source
    }

    // Add context about which function was being inlined
    errcontext("SQL function \"%s\" during inlining", callback_arg->proname);
}
```