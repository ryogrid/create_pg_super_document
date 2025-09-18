# unicodeStyleRowFormat

## Location
src/fe_utils/print.c: 101 - 106

## Overview
A structure that defines the formatting characters for table row elements in Unicode/UTF-8 style output for PostgreSQL frontend utilities.

## Definition


## Detailed Description
This structure holds the Unicode characters used for formatting table rows in PostgreSQL's frontend utilities. It provides the necessary characters for drawing horizontal lines and vertical connectors that attach to the right and left sides of table cells. The structure is specifically designed to support Unicode/UTF-8 table formatting, allowing for proper rendering of table borders and separators in terminal output.

## Parameters / Member Variables
- : A pointer to the Unicode character string used for drawing horizontal lines in table rows
- : An array of two character string pointers for vertical lines that connect to the right side, supporting different line styles or states
- : An array of two character string pointers for vertical lines that connect to the left side, supporting different line styles or states

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure definition)
- Called from (representative examples):
  - [unicodeStyleFormat](unicodeStyleFormat.md) (at src/fe_utils/print.c:128)
  - refresh_utf8format (at src/fe_utils/print.c:3696)

## Notes and Other Information
- This structure is part of PostgreSQL's table formatting system for frontend utilities like psql
- The dual-element arrays for vertical connectors likely support different formatting contexts or line weights
- Used in conjunction with other Unicode style structures to create complete table formatting schemes
- Located in src/fe_utils/print.c, which handles printing functionality for PostgreSQL frontend tools