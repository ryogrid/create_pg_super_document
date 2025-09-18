# listAvailableScripts

## Location
src/bin/pgbench/pgbench.c: 6144 - 6155

## Overview
Displays a list of all available built-in benchmark scripts to stderr for user reference.

## Definition
```c
static void listAvailableScripts(void)
```

## Detailed Description
The `listAvailableScripts` function provides a user-friendly way to display all available built-in benchmark scripts in pgbench. It iterates through the global `builtin_script` array and prints each script's name and description in a formatted manner. This function is typically called when users need to see what built-in scripts are available for benchmarking, such as when using the help or list functionality in pgbench.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard library)
  - lengthof (macro)
  - builtin_script (global array of BuiltinScript structures)
- Called from (representative examples):
  - findBuiltin (src/bin/pgbench/pgbench.c:6182)
  - main (src/bin/pgbench/pgbench.c:6782)

## Notes and Other Information
- Output is directed to stderr rather than stdout, following Unix convention for informational messages
- The function uses a 13-character right-aligned format for script names to ensure consistent alignment
- Built-in scripts typically include benchmarks like "tpcb-like", "simple-update", "select-only", etc.
- The function accesses the global builtin_script array which contains predefined benchmark scenarios
- Located in src/bin/pgbench/pgbench.c at lines 6144-6155
- Often called in response to command-line options that request script listing