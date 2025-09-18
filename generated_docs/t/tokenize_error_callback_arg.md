# tokenize_error_callback_arg

## Location
src/backend/libpq/hba.c: 66 - 67

## Overview
A simple structure used to pass context information for error callbacks during authentication configuration file tokenization.

## Definition
```c
typedef struct
{
    const char *filename;
    int         linenum;
} tokenize_error_callback_arg;
```

## Detailed Description
This structure serves as a parameter container for the tokenize_error_callback() function, which provides context information when reporting errors during the parsing of PostgreSQL authentication configuration files (like pg_hba.conf and pg_ident.conf). The structure holds the filename and line number where an error occurred, enabling the error reporting system to provide precise location information to administrators.

The structure is specifically designed to work with PostgreSQL's error context callback mechanism, allowing the parser to maintain and report the current parsing position when errors are encountered during configuration file processing.

## Parameters / Member Variables
- `filename`: Pointer to a constant string containing the name of the configuration file being parsed
- `linenum`: Integer representing the current line number in the file where parsing is taking place or where an error occurred

## Dependencies
- Functions called/Symbols referenced:
  - (This is a data structure with no function calls)
- Called from (representative examples):
  - tokenize_error_callback
  - tokenize_auth_file

## Notes and Other Information
- This structure is used in conjunction with PostgreSQL's ErrorContextCallback mechanism to provide detailed error location information
- The structure is typically instantiated on the stack within tokenize_auth_file() and passed to the error context system
- The filename member points to an existing string and does not own the memory
- Located in src/backend/libpq/hba.c:64-66, this is part of the HBA (Host-Based Authentication) configuration parsing subsystem
- The structure enables administrators to quickly locate problematic lines in configuration files when parsing errors occur