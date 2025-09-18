# print_troff_ms_vertical

## Location
src/fe_utils/print.c: 2919 - 3038

## Overview
A function that renders tabular data in vertical format using troff -ms markup, displaying each record as a series of field-value pairs rather than traditional horizontal rows.

## Definition
```c
static void print_troff_ms_vertical(const printTableContent *cont, FILE *fout)
```

## Detailed Description
This function formats tabular data in a vertical layout using troff -ms macros, where each record is displayed as a list of field-value pairs. Unlike horizontal table formatting, this approach is better suited for wide tables or detailed record inspection. The function handles:

1. **Record grouping**: Each record is labeled with "Record N" headers in italics
2. **Field-value pairing**: Each data cell is paired with its corresponding column header
3. **Dynamic formatting**: Uses .T& (table continuation) directives to change table formats mid-stream
4. **Border handling**: Supports different border styles with appropriate separator lines
5. **Format tracking**: Maintains state to optimize troff table format changes

The vertical format uses two-column troff tables where the first column contains field names (headers) and the second contains the corresponding values. Record boundaries are clearly marked, and formatting adapts based on border settings.

## Parameters / Member Variables
- `cont`: Pointer to printTableContent structure containing table data, formatting options, headers, cells, and metadata
- `fout`: Output file stream where the troff-formatted vertical table will be written

## Dependencies
- Functions called/Symbols referenced:
  - troff_ms_escaped_print (for escaping text content)
  - fputs, fputc, fprintf (standard C library functions)
- Called from (representative examples):
  - printTable (at src/fe_utils/print.c:3525)

## Notes and Other Information
- This is a static function, accessible only within src/fe_utils/print.c
- Uses a state machine approach with `current_format` to track table format changes (0=none, 1=header, 2=body)
- Record numbering starts from `cont->opt->prior_records + 1` to support pagination
- Handles three border levels: 0 (no borders), 1 (field separators), 2 (full box with record separators)
- Uses troff table continuation (.T&) to dynamically change column specifications
- Field names and values are both escaped using troff_ms_escaped_print
- Supports cancellation via global `cancel_pressed` variable
- Column specifications: "c l" (center, left), "c s" (center, span), "c | l" (center, vertical bar, left)
- Part of PostgreSQL's frontend printing subsystem for generating readable formatted output
- Particularly useful for displaying wide tables or detailed record examination