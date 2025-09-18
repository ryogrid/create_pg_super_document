# usage

## Location
src/bin/pgbench/pgbench.c: 870 - 950

## Overview
A static function that prints comprehensive help text for the initdb command, displaying all available command-line options and their descriptions.

## Definition
static void usage(const char *progname)

## Detailed Description
The usage function in initdb serves as the help system for the PostgreSQL database cluster initialization utility. It provides a comprehensive overview of all available command-line options, organized into logical groups including authentication options, locale settings, WAL configuration, debugging options, and general utilities. The function uses internationalization (i18n) support through the _() macro to ensure help text can be displayed in multiple languages. This function is essential for user experience, providing clear guidance on how to properly configure a new PostgreSQL database cluster during initialization.

## Parameters / Member Variables
- progname: The name of the program (typically "initdb") used in the usage examples and output formatting

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function for formatted output)
  - _() (internationalization macro for translatable strings)
  - PACKAGE_BUGREPORT (macro containing bug report contact information)
  - PACKAGE_NAME (macro containing the package name)
  - PACKAGE_URL (macro containing the project homepage URL)

- Called from (representative examples):
  - [main](../m/main.md) (when help is requested via command line options)

## Notes and Other Information
- Supports internationalization through gettext-style _() macros for translatable help text
- Organizes options into logical groups: basic options, authentication, locale settings, less common options, and general help
- Provides detailed explanations for complex options like locale configuration and WAL settings
- Includes contact information for bug reports and project homepage
- Essential for user experience and proper database cluster configuration guidance
- Uses consistent formatting and spacing to ensure readability across different terminal widths