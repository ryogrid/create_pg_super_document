# ws_options

## Location
src/bin/pg_walsummary/pg_walsummary.c: 25 - 29

## Overview
The `ws_options` structure defines configuration options for the `pg_walsummary` command-line utility, controlling output behavior when printing WAL summary file contents.

## Definition
```c
typedef struct ws_options
{
    bool        individual;
    bool        quiet;
} ws_options;
```

## Detailed Description
The `ws_options` structure is used to store command-line option settings for the `pg_walsummary` utility. This utility is designed to print the contents of WAL (Write-Ahead Log) summary files in a readable format. The structure contains boolean flags that modify the behavior of the output generation process.

The structure is initialized in the main function and populated based on command-line arguments parsed using `getopt_long`. The options control how detailed and verbose the output should be when processing WAL summary files.

## Parameters / Member Variables
- `individual`: Boolean flag that controls whether to display individual block information when processing WAL summary files (controlled by `-i` or `--individual` command-line option)
- `quiet`: Boolean flag that enables quiet mode, reducing the verbosity of output (controlled by `-q` or `--quiet` command-line option)

## Dependencies
- Functions called/Symbols referenced: None (simple data structure)
- Called from (representative examples):
  - `[main](../m/main.md)` function at src/bin/pg_walsummary/pg_walsummary.c:64 (structure instantiation and initialization)
  - `[dump_one_relation](../d/dump_one_relation.md)` function at src/bin/pg_walsummary/pg_walsummary.c:129 (passed as parameter to control output behavior)

## Notes and Other Information
- The structure is allocated on the stack in the main function and initialized to zero using `memset`
- Both flags default to false and are set to true only when corresponding command-line options are provided
- The structure is passed by pointer to functions that need to check the option settings
- This is part of the pg_walsummary utility which is used for debugging and analyzing WAL summary files in PostgreSQL