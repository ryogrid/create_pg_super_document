# pg_wcssize

## Location
src/fe_utils/mbprint.c: 211 - 293

## Overview
Calculates the display dimensions and formatting size requirements for a multibyte string, providing essential metrics for text formatting and display in PostgreSQL frontend utilities.

## Definition


## Detailed Description
pg_wcssize analyzes a multibyte character string and computes three key metrics needed for text display and formatting. It processes the string character by character, handling various control characters (newlines, tabs, carriage returns) and multibyte characters according to the specified encoding. The function is specifically designed to work in tandem with pg_wcsformat and must be kept synchronized with it.

The function handles special characters as follows:
- Newlines (): Increment height and reset line width
- Carriage returns (): Add 2 characters to width 
- Tabs (): Expand to next 8-character boundary
- Control characters: Display as escape sequences (\u0000 format for non-ASCII, 4 chars for ASCII)
- Regular characters: Add their display width

## Parameters / Member Variables
- : Input multibyte character string to analyze
- : Length of the input string in bytes
- : Character encoding identifier for proper multibyte handling
- : Output parameter for the width in display characters of the longest line
- : Output parameter for the number of lines in the display output
- : Output parameter for the number of bytes required to store the formatted representation

## Dependencies
- Functions called/Symbols referenced:
  - PQmblen: Determines the byte length of a multibyte character
  - PQdsplen: Determines the display width of a multibyte character
- Called from (representative examples):
  - print_aligned_text: For calculating table formatting dimensions
  - print_aligned_vertical: For vertical table formatting
  - lineptr: Through header inclusion for line pointer operations

## Notes and Other Information
- This function MUST be kept in sync with pg_wcsformat to ensure consistent formatting behavior
- The function accounts for null terminators in format_size calculations
- Tab expansion follows standard 8-character tab stops
- Control characters are rendered as escape sequences, requiring additional space
- The function is located in src/fe_utils/mbprint.c and is part of PostgreSQL's frontend utilities for text display