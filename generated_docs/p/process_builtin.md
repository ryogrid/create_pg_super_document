# process_builtin

## Location
src/bin/pgbench/pgbench.c: 6137 - 6143

## Overview
Parses a built-in benchmark script and adds it to the list of scripts available for pgbench execution.

## Definition
```c
static void process_builtin(const BuiltinScript *bi, int weight)
```

## Detailed Description
The `process_builtin` function serves as a wrapper that processes predefined built-in benchmark scripts in pgbench. It takes a pointer to a BuiltinScript structure containing the script content and description, then delegates to the ParseScript function to add it to the benchmark script collection with the specified weight. This function enables pgbench to use its standard built-in scripts like TPC-B, simple-update, and select-only benchmarks.

## Parameters / Member Variables
- `bi`: Pointer to a BuiltinScript structure containing the script content and description
- `weight`: Weight value assigned to this built-in script for weighted execution in benchmark runs

## Dependencies
- Functions called/Symbols referenced:
  - [ParseScript](../P/ParseScript.md)
  - [BuiltinScript](../B/BuiltinScript.md) (type)
- Called from (representative examples):
  - [main](../m/main.md) (multiple locations in src/bin/pgbench/pgbench.c: 6786, 6887, 6928, 7052)

## Notes and Other Information
- This is a simple wrapper function that provides a consistent interface for processing built-in scripts
- Built-in scripts are predefined and embedded in the pgbench binary, unlike external file scripts processed by process_file
- The function uses the script description from the BuiltinScript structure as the "filename" parameter for ParseScript
- Located in src/bin/pgbench/pgbench.c at lines 6137-6143
- Multiple calls from main() indicate this function is used to load various built-in benchmark scenarios