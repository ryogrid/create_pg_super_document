# print_csv_vertical

## Location
[src/fe_utils/print.c:1920-1951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L1920-L1951)

## Overview
Renders tabular data in vertical CSV format where each row becomes a set of column-name,column-value pairs, similar to PostgreSQL's expanded display mode but in CSV format.

## Definition
```c
static void print_csv_vertical(const printTableContent *cont, FILE *fout)
```

## Detailed Description
This function outputs PostgreSQL query results in a vertical CSV format, which transforms traditional row-column table data into a column-name,value pair format. Each cell from the original table becomes a separate CSV record consisting of the column header followed by the corresponding data value.

The function processes data sequentially:
- For each data cell, it outputs the corresponding column name (header)
- Followed by the configured field separator
- Then the actual data value  
- Each name-value pair is terminated with a newline

This format is particularly useful for tables with many columns or when a pivoted view of the data is desired. It's the CSV equivalent of PostgreSQL's `\x` (expanded display) mode, making wide tables more readable by presenting each field on its own line.

The function uses csv_print_field() to ensure both column names and values are properly escaped according to CSV standards, handling special characters, quotes, and field separators correctly.

## Parameters / Member Variables  
- `cont`: Pointer to printTableContent structure containing table data, headers, and CSV formatting options
- `fout`: File stream where the vertical CSV output will be written

## Dependencies
- Functions called/Symbols referenced:
  - [csv_print_field](../c/csv_print_field.md)
  - fputc (standard C library)
- Called from (representative examples):
  - [printTable](printTable.md)

## Notes and Other Information
This function is part of PostgreSQL's frontend utilities and provides an alternative CSV output format that complements the standard horizontal CSV format. The vertical format is especially valuable for tables with numerous columns or when examining individual records in detail. Like other PostgreSQL printing functions, it respects the cancel_pressed flag for responsive interruption during processing. The output format produces exactly two columns (column_name, column_value) regardless of the input table structure, making it suitable for further processing by tools that expect consistent CSV structure.