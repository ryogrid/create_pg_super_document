# print_troff_ms_text

## Location
src/fe_utils/print.c: 2827 - 2918

## Overview
A function responsible for rendering tabular data in troff -ms format, handling table structure, headers, data cells, borders, and footers with proper troff markup.

## Definition
```c
static void print_troff_ms_text(const printTableContent *cont, FILE *fout)
```

## Detailed Description
This function formats and outputs tabular data using troff -ms macro package syntax. It handles the complete table lifecycle including:

1. **Table setup**: Creates troff table structure (.TS directive) with proper alignment and border specifications
2. **Title formatting**: Displays table titles using .DS C (display centered) directives
3. **Header processing**: Formats column headers with italic formatting (\\fI...\\fP)
4. **Data cell rendering**: Outputs table data with proper escaping and tab separation
5. **Footer handling**: Displays table footers using .DS L (display left-aligned) directives

The function respects various formatting options including borders (0-2 levels), tuple-only output, and handles user cancellation gracefully. All text content is properly escaped through troff_ms_escaped_print to prevent troff interpretation issues.

## Parameters / Member Variables
- `cont`: Pointer to printTableContent structure containing table data, formatting options, headers, cells, and metadata
- `fout`: Output file stream where the troff-formatted table will be written

## Dependencies
- Functions called/Symbols referenced:
  - troff_ms_escaped_print (for escaping text content)
  - footers_with_default (for retrieving table footers)
  - fputs, fputc (standard C library functions)
- Called from (representative examples):
  - printTable (at src/fe_utils/print.c:3527)

## Notes and Other Information
- This is a static function, accessible only within src/fe_utils/print.c
- Handles border levels 0 (no borders), 1 (column separators), and 2 (full box borders)
- Uses troff -ms macros: .LP (left paragraph), .TS/.TE (table start/end), .DS/.DE (display start/end)
- Column alignment is determined by the `aligns` field in the content structure
- Supports cancellation via global `cancel_pressed` variable
- Headers are formatted in italics using troff font commands (\\fI for italic, \\fP for previous font)
- Tab characters are used as column separators in troff table format
- Part of PostgreSQL's frontend printing subsystem for generating formatted output