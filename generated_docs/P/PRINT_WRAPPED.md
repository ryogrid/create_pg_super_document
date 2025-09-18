# PRINT_WRAPPED

## Location
src/include/fe_utils/print.h: 39 - 42

## Overview
PRINT_WRAPPED is an enumeration value in the printFormat enum that represents a wrapped text output format for PostgreSQL's frontend printing utilities.

## Definition

(Part of enum printFormat in src/include/fe_utils/print.h:39)

## Detailed Description
PRINT_WRAPPED is one of the output formatting options available in PostgreSQL's frontend utilities, particularly for psql's table display functionality. This format is designed to handle text wrapping within table cells, allowing for better display of wide content that might otherwise extend beyond terminal width boundaries. It provides an alternative to other formatting options like aligned, unaligned, CSV, HTML, and other specialized formats.

## Parameters / Member Variables
As an enumeration constant, PRINT_WRAPPED has no parameters or member variables of its own.

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration constant)
- Called from (representative examples):
  - [_align2string](../a/_align2string.md) (src/bin/psql/command.c:4478)
  - fmt (src/bin/psql/command.c:4559)
  - [print_aligned_text](../p/print_aligned_text.md) (src/fe_utils/print.c:820)
  - [print_aligned_vertical](../p/print_aligned_vertical.md) (src/fe_utils/print.c:1463)
  - [printTable](../p/printTable.md) (src/fe_utils/print.c:3457, 3480)

## Notes and Other Information
- [PRINT_WRAPPED](PRINT_WRAPPED.md) is part of the printFormat enumeration that defines various output formats for PostgreSQL's frontend table printing system
- This format is particularly useful when dealing with tables that contain wide text content that needs to be displayed within terminal width constraints
- The enum comment suggests that additional output formats can be added after this value
- Used primarily in psql command-line interface for formatting query results and table outputs