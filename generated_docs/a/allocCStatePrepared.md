# allocCStatePrepared

## Location
src/bin/pgbench/pgbench.c: 3069 - 3088

## Overview
The allocCStatePrepared function allocates memory for tracking prepared statement status across all commands in all scripts for a connection state.

## Definition
```c
static void allocCStatePrepared(CState *st)
```

## Detailed Description
This function initializes the prepared statement tracking array for a connection state. It allocates a two-dimensional boolean array structure where the first dimension represents scripts and the second dimension represents commands within each script. Each boolean indicates whether the corresponding command has been prepared as a prepared statement.

The allocation happens in two phases:
1. Allocate an array of pointers, one for each script
2. For each script, count its commands and allocate a boolean array to track preparation status

The function uses pg_malloc0 to ensure the boolean array is zero-initialized, meaning all commands start as unprepared.

## Parameters / Member Variables
- `st`: Pointer to CState structure representing a client connection state where the prepared array will be allocated

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc (for allocating the script pointer array)
  - pg_malloc0 (for allocating zero-initialized boolean arrays)
  - [CState](../C/CState.md) (client state structure)
  - [ParsedScript](../P/ParsedScript.md) (script structure containing commands)
- Called from (representative examples):
  - [prepareCommand](../p/prepareCommand.md)
  - [prepareCommandsInPipeline](../p/prepareCommandsInPipeline.md)

## Notes and Other Information
- The function includes an assertion to ensure the prepared array is not already allocated, preventing memory leaks
- Memory allocation follows PostgreSQL conventions using pg_malloc family functions
- The structure allows efficient tracking of which commands have been prepared across multiple scripts
- This is essential for pgbench's prepared statement optimization feature