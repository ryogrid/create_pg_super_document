# finish_xact_command

## Location
src/backend/tcop/postgres.c: 2798 - 2829

## Overview
A convenience function that commits a transaction command and performs cleanup operations including timeout disabling and optional memory context checking.

## Definition


## Detailed Description
This function completes a transaction command by disabling the active statement timeout and committing the transaction if one was started. It serves as the counterpart to start_xact_command() in PostgreSQL's command processing lifecycle. After committing the transaction, the function optionally performs memory context checking and statistics reporting when compiled with appropriate debugging flags. The xact_started flag is reset to false to indicate that no transaction is currently active.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [disable_statement_timeout](../d/disable_statement_timeout.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [MemoryContextCheck](../M/MemoryContextCheck.md) (when MEMORY_CONTEXT_CHECKING is defined)
  - [MemoryContextStats](../M/MemoryContextStats.md) (when SHOW_MEMORY_STATS is defined)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_execute_message](../e/exec_execute_message.md)
  - [PostgresMain](../P/PostgresMain.md)

## Notes and Other Information
- Always disables statement timeout regardless of transaction state
- Only commits if a transaction was actually started (checked via xact_started flag)
- Includes optional memory debugging features for development builds
- Memory context checking helps detect memory leaks and corruption
- Memory statistics can be used for performance analysis and leak tracking
- Part of PostgreSQL's transaction management system paired with start_xact_command()