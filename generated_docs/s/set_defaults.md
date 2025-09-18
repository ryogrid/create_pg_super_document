# set_defaults

## Location
[src/tools/pg_bsd_indent/args.c:246-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/args.c#L246-L260)

## Overview
The set_defaults function initializes all indentation parameters to their default values by iterating through the parameter configuration table.

## Definition
```c
void set_defaults(void)
```

## Detailed Description
This function performs the initialization of the PostgreSQL BSD indent tool's configuration system by setting all parameters to their predefined default values. The function operates in two phases:

1. **Special case handling**: Manually sets ps.case_indent to 0.0 because it's a float value that cannot be initialized directly from the configuration table
2. **Table-driven initialization**: Iterates through the global `pro` array (parameter configuration table) and sets each parameter to its default value, except for PRO_SPECIAL type parameters

The function uses the parameter configuration structure where each entry contains the parameter name, type, default value, and a pointer to the actual variable. This table-driven approach ensures consistent initialization and makes it easy to add new parameters.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pro (global parameter configuration table)
  - PRO_SPECIAL (constant indicating special parameter types)
  - ps.case_indent (global parser state variable for case statement indentation)
- Called from (representative examples):
  - [main](../m/main.md) (src/tools/pg_bsd_indent/indent.c:187)

## Notes and Other Information
- The function handles the special case of ps.case_indent (float) separately because floating-point values cannot be initialized in static data structures in the same way as integers
- The PRO_SPECIAL parameter type is skipped during the loop, allowing for parameters that require custom initialization logic
- This function is typically called early in program initialization before processing command-line arguments or profile files
- The table-driven approach makes the parameter system extensible and maintainable
- All global configuration variables are reset to known good states, ensuring reproducible behavior