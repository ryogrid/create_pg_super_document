# BuiltinScript

## Location
src/bin/pgbench/pgbench.c: 773 - 778

## Overview
BuiltinScript is a structure that defines predefined test scripts available in pgbench for database performance benchmarking.

## Definition
```c
typedef struct BuiltinScript
{
    const char *name;        /* very short name for -b ... */
    const char *desc;        /* short description */
    const char *script;      /* actual pgbench script */
} BuiltinScript;
```

## Detailed Description
BuiltinScript represents a complete test scenario that can be executed by pgbench using the -b command-line option. Each builtin script contains a concise name for easy reference, a human-readable description explaining its purpose, and the actual SQL script content that will be executed during benchmarking. These scripts provide standardized performance tests for common database operations like TPC-B-like transactions, simple selects, and other typical workloads.

## Parameters / Member Variables
- `name`: Short identifier used with the -b command-line flag to select this script
- `desc`: Human-readable description of what the script tests or demonstrates
- `script`: The complete pgbench script text containing SQL commands and meta-commands

## Dependencies
- Functions called/Symbols referenced:
  - No direct function calls (data structure only)
- Called from (representative examples):
  - [process_builtin](../p/process_builtin.md)() at src/bin/pgbench/pgbench.c:6137
  - [listAvailableScripts](../l/listAvailableScripts.md)() at src/bin/pgbench/pgbench.c:6155
  - [findBuiltin](../f/findBuiltin.md)() at src/bin/pgbench/pgbench.c:6161
  - [main](../m/main.md)() at src/bin/pgbench/pgbench.c:6994

## Notes and Other Information
- Builtin scripts provide standardized benchmarks for consistent performance testing across different PostgreSQL installations
- The structure enables easy addition of new predefined test scenarios without modifying the core pgbench logic
- Scripts are typically stored in a static array and accessed by name during command-line processing
- Each script follows pgbench's scripting format which supports SQL commands, variable substitution, and meta-commands
- Located in src/bin/pgbench/pgbench.c:773-778