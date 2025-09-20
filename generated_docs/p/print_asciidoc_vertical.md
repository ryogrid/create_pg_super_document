# print_asciidoc_vertical

## Location
[src/fe_utils/print.c:2296-2391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2296-L2391)

## Overview
Prints table data in AsciiDoc vertical format where each record is displayed as a series of field-value pairs in a vertical layout.

## Definition

```c
static void
print_asciidoc_vertical(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function formats and outputs tabular data in AsciiDoc vertical format, where instead of displaying data in traditional columns and rows, each record is presented vertically with field names and their corresponding values. The function handles AsciiDoc-specific formatting including table headers, borders, cell alignment, and footer information. It supports various border styles (none, partial, full) and can optionally include record numbers and titles.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing the table data, headers, formatting options, and metadata
- : File stream where the formatted AsciiDoc output will be written

## Dependencies
- Functions called/Symbols referenced:
  - [asciidoc_escaped_print](../a/asciidoc_escaped_print.md) (for escaping special AsciiDoc characters in content)
  - [printTableContent](printTableContent.md) (data structure)
  - [printTableFooter](printTableFooter.md) (data structure)
  - cancel_pressed (global variable for interrupt handling)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- This is a static function within print.c, indicating it's used internally for AsciiDoc formatting
- Supports different border styles: 0 (no borders), 1 (no frame), 2 (full borders and grid)
- Handles cancellation via cancel_pressed global variable for responsive interruption
- Uses AsciiDoc table syntax with |==== delimiters and column specifications
- Record numbering starts from cont->opt->prior_records + 1 to support pagination
- Empty or whitespace-only cells are rendered as single space to maintain table structure
- Footers are displayed in a literal block (....)