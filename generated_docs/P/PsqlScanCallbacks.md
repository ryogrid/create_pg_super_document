# PsqlScanCallbacks

## Location
src/include/fe_utils/psqlscan.h: 61 - 67

## Overview
PsqlScanCallbacks is a structure that defines callback functions used by PostgreSQL's SQL lexer for variable substitution during parsing operations.

## Definition
```c
typedef struct PsqlScanCallbacks
{
    /* Fetch value of a variable, as a free'able string; NULL if unknown */
    /* This pointer can be NULL if no variable substitution is wanted */
    char *(*get_variable) (const char *varname, PsqlScanQuoteType quote,
                          void *passthrough);
} PsqlScanCallbacks;
```

## Detailed Description
PsqlScanCallbacks is a function pointer structure that provides a callback interface for the PostgreSQL SQL lexer to perform variable substitution during parsing. This structure allows the calling application to customize how variables are resolved and formatted when encountered during SQL scanning operations. The primary use case is in PostgreSQL frontend utilities like psql, where variables (such as \set variables) need to be expanded during command processing.

The callback mechanism provides flexibility for different applications to implement their own variable resolution logic while maintaining a consistent interface with the lexer. The structure supports optional variable substitution - if no substitution is desired, the callback pointer can be set to NULL.

## Parameters / Member Variables
- `get_variable`: Function pointer for variable retrieval with the following parameters:
  - `varname`: The name of the variable to retrieve
  - `quote`: A PsqlScanQuoteType value indicating how the retrieved value should be quoted
  - `passthrough`: A void pointer for passing additional context to the callback
  - Returns: A malloc'd string containing the variable value, or NULL if the variable is unknown

## Dependencies
- Functions called/Symbols referenced:
  - get_variable (function pointer)
  - PsqlScanQuoteType
- Called from (representative examples):
  - PsqlScanStateData (src/include/fe_utils/psqlscan_int.h:130)
  - MAINLOOP_H (src/bin/psql/mainloop.h:13)

## Notes and Other Information
- The get_variable callback function must return a malloc'd string that the caller will free
- The quote parameter allows the callback to format the returned value appropriately for different SQL contexts (plain text, SQL literal, SQL identifier, or shell argument)
- Setting the get_variable pointer to NULL disables variable substitution entirely
- This callback mechanism is essential for implementing psql's \set variable functionality
- The passthrough parameter allows for context-specific data to be passed to the callback function without modifying the callback signature