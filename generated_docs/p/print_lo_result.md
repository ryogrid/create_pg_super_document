# print_lo_result

## Location
src/bin/psql/large_obj.c: 16 - 55

## Overview
A utility function in psql's large object module that handles formatted output of large object operation results, supporting both console output and logging with HTML format awareness.

## Definition


## Detailed Description
This static function provides a centralized mechanism for outputting formatted messages related to large object operations in psql. It handles dual output streams - both to the query output file and to the log file if logging is enabled. The function is format-aware, automatically wrapping output in HTML paragraph tags when HTML output format is selected. Output is conditional based on the quiet setting, allowing suppression of messages when needed.

## Parameters / Member Variables
- : Format string for printf-style formatting
- : Variable arguments corresponding to the format string placeholders

## Dependencies
- Functions called/Symbols referenced:
  - pg_attribute_printf (GCC attribute for printf format checking)
  - vfprintf (standard C library function for formatted output)
  - PRINT_HTML (enum value for HTML output format)
- Called from (representative examples):
  - [do_lo_export](../d/do_lo_export.md)
  - [do_lo_import](../d/do_lo_import.md)  
  - [do_lo_unlink](../d/do_lo_unlink.md)

## Notes and Other Information
- Function is marked static, limiting scope to the large_obj.c file
- Uses pg_attribute_printf(1, 2) for compile-time format string validation
- Respects pset.quiet setting to allow suppression of output
- Automatically handles HTML formatting when output format is HTML
- Supports dual output to both query output stream and log file
- Part of psql's large object management subsystem