# set_option

## Location
[src/tools/pg_bsd_indent/args.c:261-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/args.c#L261-L334)

## Overview
The set_option function parses and processes individual command-line options or profile settings for the PostgreSQL BSD indent tool, handling different parameter types and special cases.

## Definition
```c
void set_option(char *arg)
```

## Detailed Description
This function serves as the central option processing engine for the PostgreSQL BSD indent tool. It implements a table-driven parameter parsing system that:

1. **Option Matching**: Searches through the global `pro` configuration table to find a parameter matching the input argument using the `eqin` prefix matching function
2. **Parameter Type Dispatch**: Based on the parameter type (PRO_SPECIAL, PRO_BOOL, PRO_INT), executes different processing logic
3. **Special Parameter Handling**: Processes special cases including:
   - IGN: Ignored parameters
   - CLI: Case indentation (float value)
   - STDIN: Standard input/output redirection
   - KEY: Custom type definitions
   - KEY_FILE: Type definitions from files
   - VERSION: Version information display
4. **Type-Safe Processing**: Validates and converts parameter values according to their expected types
5. **Error Handling**: Provides detailed error messages when parameters are missing or invalid

The function expects arguments to start with "-" (which it skips) and uses the global `option_source` variable for error reporting context.

## Parameters / Member Variables
- `arg`: A string containing the command-line option to be processed, typically starting with "-"

## Dependencies
- Functions called/Symbols referenced:
  - pro (global parameter configuration table)
  - [eqin](../e/eqin.md) (prefix matching function)
  - [errx](../e/errx.md) (error reporting and exit)
  - atof (string to float conversion)
  - atoi (string to integer conversion)
  - isdigit (character classification)
  - [add_typename](../a/add_typename.md) (adds custom type names)
  - add_typedefs_from_file (loads type definitions from file)
  - printf (standard output)
  - exit (program termination)
  - Various constants: PRO_SPECIAL, PRO_BOOL, PRO_INT, IGN, CLI, STDIN, KEY, KEY_FILE, VERSION, OFF, INDENT_VERSION
- Called from (representative examples):
  - [scan_profile](scan_profile.md) (src/tools/pg_bsd_indent/args.c:225)
  - [main](../m/main.md) (src/tools/pg_bsd_indent/indent.c:218)

## Notes and Other Information
- The function modifies global state variables pointed to by the parameter configuration table entries
- Error messages include the `option_source` context (e.g., "Command line", profile file name)
- The VERSION special parameter causes immediate program termination with exit(0)
- Boolean parameters use the p_special field to determine ON/OFF state
- Integer parameters require digit validation before conversion
- The function handles both short options and options with embedded values
- Special handling for ps.case_indent as a float value that cannot be table-driven
- Comprehensive error checking with descriptive error messages for debugging