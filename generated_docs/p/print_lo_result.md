# print_lo_result

## Location
[src/bin/psql/large_obj.c:16-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/large_obj.c#L16-L55)

## Overview
A utility function in psql's large object module that handles formatted output of large object operation results, supporting both console output and logging with HTML format awareness.

## Definition

```c
#include "settings.h"

static void print_lo_result(const char *fmt,...) pg_attribute_printf(1, 2);

static void
print_lo_result(const char *fmt,...)
```
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

## Simplified Source

```c
static void print_lo_result(const char *fmt, ...) {
    va_list ap;

    // Output to console unless quiet mode is set
    if (!pset.quiet) {
        // Add HTML paragraph tags if in HTML format
        if (pset.popt.topt.format == PRINT_HTML) {
            fputs("<p>", pset.queryFout);
        }

        // Print formatted message
        va_start(ap, fmt);
        vfprintf(pset.queryFout, fmt, ap);
        va_end(ap);

        // Close HTML tags or add newline
        if (pset.popt.topt.format == PRINT_HTML) {
            fputs("</p>\n", pset.queryFout);
        } else {
            fputs("\n", pset.queryFout);
        }
    }

    // Also log to file if logging is enabled
    if (pset.logfile) {
        va_start(ap, fmt);
        vfprintf(pset.logfile, fmt, ap);
        va_end(ap);
        fputs("\n", pset.logfile);
    }
}
```