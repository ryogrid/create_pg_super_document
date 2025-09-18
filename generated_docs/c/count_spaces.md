# count_spaces

## Location
src/tools/pg_bsd_indent/io.c: 550 - 555

## Overview
A simple wrapper function that calculates the final column position after printing an entire null-terminated string.

## Definition


## Detailed Description
The  function is a convenience wrapper around  in the PostgreSQL BSD indent tool. It provides a simpler interface for calculating column positions when processing complete null-terminated strings, eliminating the need to specify an end boundary.

This function simply delegates to  with a NULL end pointer, which causes the underlying function to process the entire string until it encounters a null terminator. This makes it ideal for cases where the entire buffer content needs to be analyzed for positioning calculations.

## Parameters / Member Variables
- : The current column position to start calculation from (integer)
- : Pointer to the null-terminated character buffer to analyze

## Dependencies
- Functions called/Symbols referenced:
  - : The core function that performs the actual column calculation
- Called from (representative examples):
  - : Used for line formatting and positioning calculations
  - : Used in code alignment computations
  - : Used in comment processing for alignment

## Notes and Other Information
- This is a thin wrapper that provides a more convenient API for common use cases
- Inherits all the character processing logic from  (tabs, newlines, backspace, etc.)
- Returns the final column position after processing the entire string
- More convenient than  when processing complete strings
- Essential for layout calculations throughout the indent tool
- Part of the original codebase design from 1976, providing a clean API separation between bounded and unbounded string processing