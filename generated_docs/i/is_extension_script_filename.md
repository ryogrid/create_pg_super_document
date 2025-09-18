# is_extension_script_filename

## Location
src/backend/commands/extension.c: 368 - 375

## Overview
Utility function that determines whether a given filename represents an extension script file by checking if it ends with the ".sql" extension.

## Definition
static bool is_extension_script_filename(const char *filename)

## Detailed Description
This static helper function performs filename validation to identify PostgreSQL extension script files. Extension script files contain SQL commands for creating, updating, or dropping extensions. These files typically include installation scripts (e.g., extension--1.0.sql), upgrade scripts (e.g., extension--1.0--1.1.sql), and other SQL-based extension operations. The function uses string manipulation to locate the file extension and compares it against the expected ".sql" suffix.

The function is part of PostgreSQL's extension management infrastructure and serves as a filtering mechanism when scanning directories for extension-related SQL script files.

## Parameters / Member Variables
- `filename`: A null-terminated string containing the filename to check for the ".sql" extension

## Dependencies
- Functions called/Symbols referenced:
  - strrchr (standard C library function)
  - strcmp (standard C library function)
- Called from (representative examples):
  - get_ext_ver_list

## Notes and Other Information
- This is a static function, meaning it's only accessible within the src/backend/commands/extension.c file
- The function performs case-sensitive comparison, so ".SQL" would not be recognized
- Returns true only if the filename ends exactly with ".sql"
- Used primarily during extension version discovery when scanning for available script files
- Extension script files follow naming conventions like extension--version.sql or extension--oldver--newver.sql for upgrade scripts