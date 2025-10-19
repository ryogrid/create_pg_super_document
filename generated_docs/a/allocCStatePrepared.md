# allocCStatePrepared

## Location
[src/bin/pgbench/pgbench.c:3069-3088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3069-L3088)

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
  - [pg_malloc](../p/pg_malloc.md) (for allocating the script pointer array)
  - [pg_malloc0](../p/pg_malloc0.md) (for allocating zero-initialized boolean arrays)
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

## Simplified Source

```c
static void allocCStatePrepared(CState *st) {
    Assert(st->prepared == NULL);

    // Allocate array of pointers, one for each script
    st->prepared = pg_malloc(sizeof(bool *) * num_scripts);

    // For each script, allocate array to track command preparation status
    for (int i = 0; i < num_scripts; i++) {
        ParsedScript *script = &sql_script[i];

        // Count commands in this script
        int numcmds = 0;
        while (script->commands[numcmds] != NULL)
            numcmds++;

        // Allocate zero-initialized boolean array for this script's commands
        st->prepared[i] = pg_malloc0(sizeof(bool) * numcmds);
    }
}
```