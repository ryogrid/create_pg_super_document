# _print_horizontal_line

## Location
[src/fe_utils/print.c:593-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L593-L634)

## Overview
Draws horizontal lines (borders) for aligned text table formatting with support for different border styles and positioning rules.

## Definition

```c
static void
_print_horizontal_line(const unsigned int ncolumns, const unsigned int *widths,
					   unsigned short border, printTextRule pos,
					   const printTextFormat *format,
					   FILE *fout)
```
## Detailed Description
This utility function generates horizontal lines used to create borders and separators in aligned text table output. It supports different border styles (no border, single border, double border) and various horizontal line positions (top, middle, bottom) as defined by the printTextRule. The function constructs lines using formatting characters from the printTextFormat structure, properly handling column widths and junction points where horizontal and vertical rules meet.

## Parameters / Member Variables
- `ncolumns`: Number of columns in the table
- `*widths`: Array containing the width of each column in characters
- `border`: Border style flag (0=none, 1=single, 2=double)
- `pos`: Position of the horizontal line (top, middle, bottom) as defined by printTextRule enum
- `*format`: Pointer to printTextFormat structure containing formatting characters for different line types
- `*fout`: FILE pointer to the output stream where the line will be written
## Dependencies
- Functions called/Symbols referenced:
  - [printTextLineFormat](printTextLineFormat.md) (structure for line formatting rules)
  - [printTextRule](printTextRule.md) (enum for line position types)
  - [printTextFormat](printTextFormat.md) (structure containing all formatting rules)
  - fputs (standard C library function for string output)
  - fprintf (standard C library function for formatted output)
  - fputc (standard C library function for character output)
- Called from (representative examples):
  - [print_aligned_text](print_aligned_text.md) (for drawing table borders and separators)

## Notes and Other Information
- Function name starts with underscore, indicating it's an internal helper function
- Handles three different border styles: no border, single border lines, and double border lines
- Uses different characters for horizontal rules, left/right vertical rules, and middle vertical rules based on the format specification
- Constructs lines by repeating horizontal rule characters for each column's width
- Properly handles junction points between columns using midvrule characters
- Always terminates the line with a newline character
- Function is static, indicating it's only used within the print.c module as part of the table formatting subsystem
- Essential for creating properly formatted aligned text tables with consistent borders and separators

## Simplified Source

```c
static void _print_horizontal_line(const unsigned int ncolumns, const unsigned int *widths,
                                   unsigned short border, printTextRule pos,
                                   const printTextFormat *format, FILE *fout) {
    const printTextLineFormat *lformat = &format->lrule[pos];

    // Print left border based on border style
    if (border == 1) {
        fputs(lformat->hrule, fout);
    } else if (border == 2) {
        fprintf(fout, "%s%s", lformat->leftvrule, lformat->hrule);
    }

    // Print horizontal line for each column
    for (unsigned int i = 0; i < ncolumns; i++) {
        // Fill column width with horizontal rule chars
        for (unsigned int j = 0; j < widths[i]; j++) {
            fputs(lformat->hrule, fout);
        }

        // Print junction between columns (except after last column)
        if (i < ncolumns - 1) {
            if (border == 0) {
                fputc(' ', fout);
            } else {
                fprintf(fout, "%s%s%s", lformat->hrule,
                        lformat->midvrule, lformat->hrule);
            }
        }
    }

    // Print right border based on border style
    if (border == 2) {
        fprintf(fout, "%s%s", lformat->hrule, lformat->rightvrule);
    } else if (border == 1) {
        fputs(lformat->hrule, fout);
    }

    fputc('\n', fout);
}
```