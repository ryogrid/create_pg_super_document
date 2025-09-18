# print_asciidoc_text

## Location
src/fe_utils/print.c: 2186 - 2295

## Overview
Renders table data in AsciiDoc table format, generating properly formatted AsciiDoc markup with configurable borders, column alignment, and headers for PostgreSQL query results.

## Definition


## Detailed Description
This function generates AsciiDoc table output from PostgreSQL query results stored in a printTableContent structure. It creates properly formatted AsciiDoc tables using the standard table block syntax with pipe delimiters and table attributes. The function handles the complete table lifecycle including table definition blocks, headers, data rows, and footers. It supports various border styles, column alignments, and formatting options. The function uses AsciiDoc-specific escaping for content and implements AsciiDoc's table formatting conventions including column specifications, frame options, and grid settings.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing table data, headers, formatting options, and configuration
- : Output file stream where the AsciiDoc table will be written

## Dependencies
- Functions called/Symbols referenced:
  - [asciidoc_escaped_print](../a/asciidoc_escaped_print.md) (for escaping AsciiDoc content)
  - [footers_with_default](../f/footers_with_default.md) (for retrieving table footers)
  - fprintf, fputs (standard C library functions)
  - strspn (standard C library function)
- Called from:
  - [printTable](printTable.md) (src/fe_utils/print.c:3509)

## Notes and Other Information
- Uses AsciiDoc table block syntax with |==== delimiters
- Generates proper column specifications with alignment indicators (<l for left, >l for right)
- Supports different border styles via frame and grid attributes (none, frame-only, or full)
- Headers use ^l| prefix for center alignment when not in tuples-only mode
- Creates table title using AsciiDoc's title syntax (leading dot)
- Empty cells are handled by outputting just the pipe delimiter with spacing
- All content is AsciiDoc-escaped to prevent formatting conflicts
- Footers are rendered in literal blocks using .... delimiters
- Includes cancellation support during processing
- Enforces proper AsciiDoc spacing and formatting conventions
- Table definition includes header option when headers are present