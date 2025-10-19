# print_aligned_vertical_line

## Location
[src/fe_utils/print.c:1225-1323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L1225-L1323)

## Overview
Draws horizontal separating lines for aligned vertical table format, including record separators with optional record numbers and proper border formatting.

## Definition

```c
struct lineptr *hlineptr,
			   *dlineptr;
```
## Detailed Description
This utility function generates horizontal separator lines specifically for vertical (record-oriented) table layouts. It creates lines that separate individual records and includes optional record numbering ("* Record N" or "[ RECORD N ]"). The function handles different border styles and can dynamically adjust line width based on terminal width constraints and expanded header width settings. It supports various header width modes including page-width, exact-width, and column-width formatting.

## Parameters / Member Variables
- : Pointer to printTableOpt structure containing table formatting options and settings
- : Record number to display in the separator (0 means no record number)
- : Width allocated for the header portion of the line
- : Width allocated for the data portion of the line  
- : Available terminal width for output formatting
- : Position type of the line (top, middle, bottom) as defined by printTextRule enum
- : FILE pointer to the output stream where the line will be written

## Dependencies
- Functions called/Symbols referenced:
  - [get_line_style](../g/get_line_style.md) (gets formatting characters for the current line style)
  - [printTextLineFormat](printTextLineFormat.md) (structure for line formatting rules)
  - [printTextRule](printTextRule.md) (enum for line position types)
  - [printTableOpt](printTableOpt.md) (structure containing table options)
  - PRINT_XHEADER_COLUMN, PRINT_XHEADER_PAGE, PRINT_XHEADER_EXACT_WIDTH (constants for header width modes)
  - fprintf (standard C library function for formatted output)
  - fputs (standard C library function for string output)
  - fputc (standard C library function for character output)
- Called from (representative examples):
  - [print_aligned_vertical](print_aligned_vertical.md) (for drawing record separators in vertical table format)

## Notes and Other Information
- Specialized function for vertical table layout, complementing _print_horizontal_line for horizontal layouts
- Handles record numbering with different formats based on border style ("* Record N" vs "[ RECORD N ]")
- Supports dynamic width adjustment based on terminal width and header width type settings
- Uses different border characters (leftvrule, rightvrule, midvrule, hrule) based on formatting rules
- Implements intelligent width calculations that respect terminal boundaries while maintaining proper alignment
- Handles three header width modes: column-based, page-based, and exact width specification  
- Function is static, indicating it's only used within the print.c module as part of the table formatting subsystem
- Essential for creating properly formatted record separators in PostgreSQL's expanded (vertical) display mode
- Provides visual separation between records when displaying wide tables in vertical format

## Simplified Source

```c
static void print_aligned_vertical_line(const printTableOpt *topt,
                                        unsigned long record,
                                        unsigned int hwidth,
                                        unsigned int dwidth,
                                        int output_columns,
                                        printTextRule pos,
                                        FILE *fout) {
    const printTextLineFormat *lformat = &get_line_style(topt)->lrule[pos];
    const unsigned short opt_border = topt->border;
    int reclen = 0;

    // Print left border
    if (opt_border == 2) {
        fprintf(fout, "%s%s", lformat->leftvrule, lformat->hrule);
    } else if (opt_border == 1) {
        fputs(lformat->hrule, fout);
    }

    // Print record number if provided
    if (record) {
        if (opt_border == 0) {
            reclen = fprintf(fout, "* Record %lu", record);
        } else {
            reclen = fprintf(fout, "[ RECORD %lu ]", record);
        }
    }
    if (opt_border != 2) reclen++;
    if (reclen < 0) reclen = 0;

    // Fill header width with appropriate characters
    for (unsigned int i = reclen; i < hwidth; i++) {
        fputs(opt_border > 0 ? lformat->hrule : " ", fout);
    }
    reclen -= hwidth;

    // Handle middle section based on border style
    if (opt_border > 0) {
        if (reclen-- <= 0) fputs(lformat->hrule, fout);
        if (reclen-- <= 0) {
            if (topt->expanded_header_width_type == PRINT_XHEADER_COLUMN) {
                fputs(lformat->rightvrule, fout);
            } else {
                fputs(lformat->midvrule, fout);
            }
        }
        if (reclen-- <= 0 && topt->expanded_header_width_type != PRINT_XHEADER_COLUMN) {
            fputs(lformat->hrule, fout);
        }
    } else {
        if (reclen-- <= 0) fputc(' ', fout);
    }

    // Handle data width section for non-column header types
    if (topt->expanded_header_width_type != PRINT_XHEADER_COLUMN) {
        // Apply width constraints based on header width type
        if (topt->expanded_header_width_type == PRINT_XHEADER_PAGE ||
            topt->expanded_header_width_type == PRINT_XHEADER_EXACT_WIDTH) {

            if (topt->expanded_header_width_type == PRINT_XHEADER_EXACT_WIDTH) {
                output_columns = topt->expanded_header_exact_width;
            }

            if (output_columns > 0) {
                // Calculate maximum data width based on border style
                if (opt_border == 0) {
                    dwidth = Min(dwidth, Max(0, (int)(output_columns - hwidth)));
                } else if (opt_border == 1) {
                    dwidth = Min(dwidth, Max(0, (int)(output_columns - hwidth - 3)));
                } else if (opt_border == 2) {
                    dwidth = Min(dwidth, Max(0, (int)(output_columns - hwidth - 7)));
                }
            }
        }

        // Fill data width
        if (reclen < 0) reclen = 0;
        if (dwidth < reclen) dwidth = reclen;

        for (unsigned int i = reclen; i < dwidth; i++) {
            fputs(opt_border > 0 ? lformat->hrule : " ", fout);
        }

        if (opt_border == 2) {
            fprintf(fout, "%s%s", lformat->hrule, lformat->rightvrule);
        }
    }

    fputc('\n', fout);
}
```