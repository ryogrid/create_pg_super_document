# footers_with_default

## Location
src/fe_utils/print.c: 398 - 421

## Overview
Returns either explicitly-requested table footers or generates a default row count footer based on table printing options and content.

## Definition


## Detailed Description
The  function determines what footer text should be displayed for a table output. It implements logic to either return explicitly provided footers from the table content or generate a default footer showing the row count. When no explicit footers are provided and default footer display is enabled, the function creates a localized "(xx rows)" or "(xx row)" message using the total record count (including prior records). The function uses  for proper pluralization based on the locale. The default footer is omitted when explicit footers are provided, when footer display is disabled, or when specifically instructed by calling commands. Vertical formats typically don't use this function since they number rows individually.

## Parameters / Member Variables
- : Pointer to a  structure containing:
  - : Array of explicit footer strings (may be NULL)
  - : Table printing options including  flag and 
  - : Number of rows in the current table

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (standard C library function)
  - ngettext (internationalization function for pluralization)
- Global variables referenced:
  - default_footer (static buffer for default footer text)
  - default_footer_cell (static footer structure)
- Called from (representative examples):
  - print_unaligned_text
  - print_aligned_text
  - print_aligned_vertical
  - print_html_text
  - print_asciidoc_text
  - print_latex_text
  - print_troff_ms_text

## Notes and Other Information
- This is a static function, only accessible within src/fe_utils/print.c
- Returns a pointer that may point to static storage and should not be kept across calls
- Supports internationalization through ngettext() for proper singular/plural forms
- The default footer format is "(N row)" or "(N rows)" depending on the count
- Total record count includes both current table rows and any prior records
- Vertical format tables don't typically call this function since they number rows individually
- The function respects user preferences for footer display through the options structure