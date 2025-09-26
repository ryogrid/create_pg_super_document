# tsvector_unnest

## Location
[src/backend/utils/adt/tsvector_op.c:632-719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L632-L719)

## Overview
Expands a TSVector into a table with separate columns for lexemes, positions, and weights, implementing a set-returning function for detailed TSVector analysis.

## Definition
```c
Datum tsvector_unnest(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a set-returning function (SRF) that unnests a TSVector into a tabular format with three columns: lexeme (text), positions (integer array), and weights (text array). It allows detailed inspection of TSVector contents by exposing the internal structure in a user-friendly format.

The function uses PostgreSQL's SRF framework to iterate through each lexeme in the TSVector. For each lexeme, it extracts the lexeme text, and if position information is available, it separates the combined position-weight values into distinct position and weight arrays. Positions are extracted as 14-bit values and weights are converted from internal numeric representation to character form ('A', 'B', 'C', 'D').

The function creates a tuple descriptor with three columns and processes each lexeme entry sequentially, handling cases where position information may not be present (setting positions and weights to NULL).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Input TSVector to unnest into tabular format

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL - Check if this is the first call of SRF
  - SRF_FIRSTCALL_INIT - [Initialize](../I/Initialize.md) SRF context  
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) - Create tuple descriptor template
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md) - [Initialize](../I/Initialize.md) tuple descriptor column
  - [get_call_result_type](../g/get_call_result_type.md) - Verify return type
  - PG_GETARG_TSVECTOR_COPY - Extract and copy TSVector argument
  - SRF_PERCALL_SETUP - Setup for each SRF call
  - ARRPTR - Get pointer to WordEntry array
  - STRPTR - Get pointer to string data
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) - Convert C string to PostgreSQL text
  - _POSVECPTR - Get position vector pointer
  - WEP_GETPOS - Extract position from position-weight value
  - WEP_GETWEIGHT - Extract weight from position-weight value
  - [construct_array_builtin](../c/construct_array_builtin.md) - Build PostgreSQL array
  - [heap_form_tuple](../h/heap_form_tuple.md) - Create heap tuple
  - SRF_RETURN_NEXT - Return next row in SRF
  - SRF_RETURN_DONE - Signal SRF completion
- Called from (representative examples):
  - No direct references found (called through SQL function dispatch as table function)

## Notes and Other Information
- Returns a table with columns: lexeme (text), positions (int2[]), weights (text[])
- Uses PostgreSQL's Set Returning Function (SRF) framework for row-by-row processing
- Handles lexemes without position information by setting positions and weights to NULL  
- Converts internal weight representation (0-3) to character form ('D'-'A') where D=lowest, A=highest weight
- Extracts 14-bit positions from the combined 16-bit position-weight storage format
- Useful for debugging and detailed analysis of TSVector contents
- Part of PostgreSQL's full-text search functionality for TSVector inspection