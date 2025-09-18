# diag2

## Location
src/tools/pg_bsd_indent/io.c: 590 - 605

## Overview
A diagnostic message output function in the pg_bsd_indent tool that formats and prints error or warning messages without additional arguments.

## Definition
```c
void diag2(int level, const char *msg)
```

## Detailed Description
The `diag2` function is a core diagnostic output utility for the pg_bsd_indent tool (PostgreSQL's BSD-style code indentation utility). It handles the formatting and output of diagnostic messages that do not require additional arguments. The function is responsible for managing both error and warning messages, with intelligent output routing based on the current output stream configuration.

When the output stream is stdout (indicating that formatted code is being written to stdout), diagnostic messages are embedded as special comments in the output stream with the format "/**INDENT** [Warning|Error]@line: message */". This prevents diagnostic messages from interfering with the formatted code output. When output is directed elsewhere, messages are sent to stderr in a standard format.

The function maintains error state management by setting the global `found_err` flag when error-level messages are encountered, allowing the indentation tool to track whether errors occurred during processing.

## Parameters / Member Variables
- `level`: Integer indicating message severity (0 for warnings, non-zero for errors)
- `msg`: The diagnostic message string to be output (used as-is without formatting)

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard library)
- Called from (representative examples):
  - main (from indent.c at multiple locations: 324, 393, 462, 483, 747, 796, 844, 853, 896, 907, 1138, 1142, 1150)
  - lexi (from lexi.c at lines 270, 470)
  - parse (from parse.c at lines 157, 176, 200)

## Notes and Other Information
- Part of the pg_bsd_indent tool's comprehensive diagnostic system
- More frequently used than `diag3`, appearing in lexical analysis, parsing, and main processing functions
- Sets global `found_err` flag to 1 when reporting errors to track processing status
- Uses special comment formatting when output stream is stdout to maintain clean code formatting
- Companion to `diag3` function which handles messages requiring integer arguments
- Essential for providing user feedback during code indentation processing