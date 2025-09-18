# ignore_slash_options

## Location
src/bin/psql/command.c: 3206 - 3222

## Overview
This function reads and discards "normal" slash command options from the input stream, used during inactive-branch processing to consume command parameters without executing them.

## Definition
```c
static void ignore_slash_options(PsqlScanState scan_state)
```

## Detailed Description
The `ignore_slash_options` function is designed for inactive-branch processing of slash commands in psql. When conditional commands like `\if` are in an inactive state, any slash commands within that branch should not be executed, but their parameters still need to be consumed from the input stream to maintain proper parsing flow.

This function specifically handles slash commands that consume one or more OT_NORMAL, OT_SQLID, or OT_SQLIDHACK parameter types. It reads all available normal options from the scanner and immediately discards them without processing. The function doesn't need to worry about consuming exactly the right number of parameters since the cleanup logic in HandleSlashCmds would silently discard any extras anyway.

The function uses a simple loop that continues calling `psql_scan_slash_option()` with the OT_NORMAL option type until no more options are available, freeing each option string as it's read.

## Parameters / Member Variables
- `scan_state`: A `PsqlScanState` structure that maintains the current state of the psql command scanner, including the input buffer and parsing position

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option`: Scans for the next slash command option from the input
  - `OT_NORMAL`: Option type constant for normal command options
  - [PsqlScanState](../P/PsqlScanState.md): Scanner state structure type

- Called from (representative examples):
  - [exec_command_bind](../e/exec_command_bind.md): When \bind commands are in inactive branches
  - [exec_command_connect](../e/exec_command_connect.md): When \connect commands are in inactive branches
  - [exec_command_set](../e/exec_command_set.md): When \set commands are in inactive branches
  - [exec_command_pset](../e/exec_command_pset.md): When \pset commands are in inactive branches

## Notes and Other Information
- This function is part of psql's conditional command processing infrastructure
- It's used extensively throughout the command processing system for inactive branch handling
- The function is static to the command.c file, indicating it's an internal utility
- Memory management is handled properly by immediately freeing each option string after reading
- The function handles the fact that different commands consume different numbers of parameters by reading until no more are available
- Works specifically with OT_NORMAL type options; other option types require different handling functions