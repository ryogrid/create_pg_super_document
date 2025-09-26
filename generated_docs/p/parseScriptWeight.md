# parseScriptWeight

## Location
[src/bin/pgbench/pgbench.c:6192-6228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L6192-L6228)

## Overview
Parses script weight specifications from command-line options and extracts the script name and weight value.

## Definition
```c
static int parseScriptWeight(const char *option, char **script)
```

## Detailed Description
The `parseScriptWeight` function processes command-line options for pgbench scripts (typically from -b and -f flags) that may include weight specifications. It handles options in the format "script_name@weight" where the weight determines the relative frequency of script execution in benchmark runs. The function splits the option string at the weight separator (WSEP, typically '@'), validates the weight value, and returns both the extracted script name (as a dynamically allocated string) and the weight as an integer. If no weight is specified, it defaults to 1.

## Parameters / Member Variables
- `option`: The command-line option string that may contain a script name and optional weight specification
- `script`: Output parameter - pointer to a char* that will be set to the malloc'd script name string

## Dependencies
- Functions called/Symbols referenced:
  - strrchr (standard library)
  - [pg_malloc](pg_malloc.md)
  - strncpy (standard library) 
  - strtol (standard library)
  - [pg_fatal](pg_fatal.md)
  - [pg_strdup](pg_strdup.md)
  - WSEP (weight separator constant)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pgbench/pgbench.c:6785, 6831)

## Notes and Other Information
- Uses WSEP (weight separator) character to separate script names from weights, typically '@'
- Performs comprehensive validation of weight values including range checking (0 to INT_MAX)
- Returns 1 as the default weight when no weight specification is provided
- The script name is dynamically allocated and must be freed by the caller
- Handles error cases with descriptive fatal error messages for invalid weight formats
- Located in src/bin/pgbench/pgbench.c at lines 6192-6228
- Essential for weighted script execution in benchmark scenarios where different scripts should run with different frequencies
- Supports both built-in script names (with findBuiltin) and external script filenames (with process_file)