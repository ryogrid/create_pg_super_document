# ParseScript

## Location
src/bin/pgbench/pgbench.c: 5941 - 5950

## Overview
ParseScript is a function that parses pgbench script content and converts it into executable commands for performance testing.

## Definition
```c
static void ParseScript(const char *script, const char *desc, int weight)
```

## Detailed Description
ParseScript processes script content (either from a file or a builtin script) and transforms it into a structured representation that pgbench can execute. The function handles the parsing of SQL commands, meta-commands (like \set, \if, etc.), and prepares the script for execution during benchmarking. It creates a ParsedScript structure containing all the parsed commands, manages memory allocation for command storage, and sets up the execution context including script description and weight for load balancing across multiple scripts.

The function uses PostgreSQL's psql scanner infrastructure to properly parse SQL statements and pgbench-specific meta-commands, ensuring correct handling of variable substitution, conditional statements, and other advanced scripting features.

## Parameters / Member Variables
- `script`: The raw script content as a string containing SQL and meta-commands
- `desc`: Human-readable description of the script for identification and reporting
- `weight`: Relative weight of this script when multiple scripts are used (for load distribution)

## Dependencies
- Functions called/Symbols referenced:
  - ParsedScript (structure for storing parsed results)
  - PsqlScanState (PostgreSQL's scanner state for parsing)
  - PQExpBufferData (buffer for line processing)
- Called from (representative examples):
  - process_file() at src/bin/pgbench/pgbench.c:6130
  - process_builtin() at src/bin/pgbench/pgbench.c:6139

## Notes and Other Information
- The function is static, indicating it's only used within the pgbench.c file
- Handles both file-based scripts and builtin script content uniformly
- Uses COMMANDS_ALLOC_NUM (128) as the initial allocation size for command arrays
- Integrates with PostgreSQL's psql scanning infrastructure for consistent SQL parsing
- Script weight enables proportional execution when running multiple scripts simultaneously
- Supports complex pgbench scripting features including variables, conditionals, and loops
- Located in src/bin/pgbench/pgbench.c:5941-5950 with implementation continuing beyond the symbol definition