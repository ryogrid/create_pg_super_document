# textcat

## Location
src/backend/utils/adt/varlena.c: 750 - 764

## Overview
Concatenates two text values and returns the resulting text as a single string.

## Definition
```c
Datum textcat(PG_FUNCTION_ARGS)
```

## Detailed Description
The `textcat` function is a PostgreSQL built-in function that performs string concatenation of two text values. It serves as a wrapper around the internal `text_catenate` function, providing the standard PostgreSQL function interface for text concatenation operations.

This function has been rewritten and updated multiple times in PostgreSQL history, with contributions from Sapa (1996), Thomas Lockhart (1997), and others. The current implementation ensures proper memory allocation for the output in all cases.

The function extracts two text arguments from the function call parameters and delegates the actual concatenation work to the internal `text_catenate` function, which handles the low-level memory management and data copying.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First text value to concatenate
  - Argument 1: Second text value to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - `[text_catenate](text_catenate.md)`: Internal function that performs the actual concatenation
  - `PG_GETARG_TEXT_PP`: Extracts text arguments with potential detoasting
  - `PG_RETURN_TEXT_P`: Returns the resulting text value

- Called from (representative examples):
  - No direct callers found in the analyzed codebase (typically called via SQL)

## Notes and Other Information
- This function is typically invoked through SQL's || concatenation operator or CONCAT function
- The implementation history shows ongoing optimization efforts over the years
- Uses the PP (Pointer to Possibly packed) variant for argument extraction, which handles packed/compressed text efficiently
- Memory allocation is handled internally by the `text_catenate` helper function
- The function maintains PostgreSQL's standard error handling and memory management patterns