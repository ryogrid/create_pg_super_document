# output_row

## Location
[src/interfaces/libpq/fe-print.c:531-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-print.c#L531-L573)

## Overview
Formats and outputs a single data row from a PostgreSQL query result with proper alignment and formatting according to the specified print options.

## Definition

```c
static void
output_row(FILE *fout, const PQprintOpt *po, const int nFields, char **fields,
		   unsigned char *fieldNotNum, int *fieldMax, char *border,
		   const int row_index)
```
## Detailed Description
The  function outputs a single row of data from a PostgreSQL query result set with appropriate formatting. It works in conjunction with the  and  functions to provide complete table output formatting. The function handles multiple output formats:

1. **HTML3 Format**: Outputs HTML table row with  and  tags, including alignment attributes
2. **Standard Format**: Includes field separators and decorative borders with proper spacing
3. **Plain Format**: Simple column-aligned output without borders

The function accesses pre-processed field data from the fields array, which was populated by , and applies formatting based on numeric content detection and maximum field widths calculated during earlier processing phases.

## Parameters / Member Variables
- : Output file stream for writing the formatted row
- : Print options structure containing formatting preferences (html3, standard, fieldSep)
- : Total number of fields (columns) in the result set
- : Pre-allocated array containing all field values, indexed by row and column
- : Array indicating which fields contain non-numeric data for alignment purposes
- : Array containing maximum width for each column for proper alignment
- : Pre-constructed border string for standard format (created by do_header)
- : Index of the current row being output

## Dependencies
- Functions called/Symbols referenced:
  - PQprintOpt (struct type)
- Called from (representative examples):
  - [winsize](../w/winsize.md) (src/interfaces/libpq/fe-print.c:290)

## Notes and Other Information
- This is a void function that performs output operations only
- Field values are accessed using the formula: 
- Handles null field values by substituting empty strings
- Applies left alignment for text fields and right alignment for numeric fields
- HTML output includes proper table cell alignment attributes
- Standard format includes decorative borders after each row
- Field separators are only added between fields, not after the last field (except in standard format)