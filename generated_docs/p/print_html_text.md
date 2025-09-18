# print_html_text

## Location
src/fe_utils/print.c: 1993 - 2081

## Overview
Renders table data in HTML format, generating a complete HTML table with headers, data cells, and optional footers for PostgreSQL query results.

## Definition


## Detailed Description
This function generates HTML table output from PostgreSQL query results stored in a printTableContent structure. It creates a properly formatted HTML table with configurable borders, alignment, and styling. The function handles the complete table lifecycle including opening table tags, headers, data rows, and closing tags with optional footers. It uses HTML escaping for all content to prevent HTML injection and ensures proper formatting. The function respects various output options like tuples-only mode and can handle cancellation during processing.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing table data, headers, formatting options, and configuration
- : Output file stream where the HTML table will be written

## Dependencies
- Functions called/Symbols referenced:
  - [html_escaped_print](../h/html_escaped_print.md) (for escaping HTML content)
  - [footers_with_default](../f/footers_with_default.md) (for retrieving table footers)
  - fprintf, fputs, fputc (standard C library functions)
  - strspn (standard C library function)
- Called from:
  - [printTable](printTable.md) (src/fe_utils/print.c:3503)

## Notes and Other Information
- Generates complete HTML table markup with configurable border and table attributes
- Handles optional table title as HTML caption element
- Creates table headers with center alignment in th elements
- Data cells use left or right alignment based on column alignment settings
- Empty or whitespace-only cells are rendered as '&nbsp;' to maintain table structure
- Supports tuples-only mode which omits headers and footers
- Includes cancellation support via cancel_pressed global variable
- Footers are rendered as paragraph elements with line breaks
- All text content is HTML-escaped to prevent markup injection