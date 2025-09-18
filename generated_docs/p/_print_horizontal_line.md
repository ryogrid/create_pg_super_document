# _print_horizontal_line

## Location
src/fe_utils/print.c: 593 - 634

## Overview
Draws horizontal lines (borders) for aligned text table formatting with support for different border styles and positioning rules.

## Definition


## Detailed Description
This utility function generates horizontal lines used to create borders and separators in aligned text table output. It supports different border styles (no border, single border, double border) and various horizontal line positions (top, middle, bottom) as defined by the printTextRule. The function constructs lines using formatting characters from the printTextFormat structure, properly handling column widths and junction points where horizontal and vertical rules meet.

## Parameters / Member Variables
- : Number of columns in the table
- : Array containing the width of each column in characters
- : Border style flag (0=none, 1=single, 2=double)
- : Position of the horizontal line (top, middle, bottom) as defined by printTextRule enum
- : Pointer to printTextFormat structure containing formatting characters for different line types
- : FILE pointer to the output stream where the line will be written

## Dependencies
- Functions called/Symbols referenced:
  - printTextLineFormat (structure for line formatting rules)
  - printTextRule (enum for line position types)
  - printTextFormat (structure containing all formatting rules)
  - fputs (standard C library function for string output)
  - fprintf (standard C library function for formatted output)
  - fputc (standard C library function for character output)
- Called from (representative examples):
  - print_aligned_text (for drawing table borders and separators)

## Notes and Other Information
- Function name starts with underscore, indicating it's an internal helper function
- Handles three different border styles: no border, single border lines, and double border lines
- Uses different characters for horizontal rules, left/right vertical rules, and middle vertical rules based on the format specification
- Constructs lines by repeating horizontal rule characters for each column's width
- Properly handles junction points between columns using midvrule characters
- Always terminates the line with a newline character
- Function is static, indicating it's only used within the print.c module as part of the table formatting subsystem
- Essential for creating properly formatted aligned text tables with consistent borders and separators