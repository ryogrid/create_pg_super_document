# SPI_finish

## Location
src/backend/executor/spi.c: 182 - 221

## Overview
SPI_finish terminates an active SPI connection, cleaning up memory contexts and restoring the previous connection state on the SPI stack.

## Definition


## Detailed Description
SPI_finish is the counterpart to SPI_connect/SPI_connect_ext that properly closes an SPI connection and performs necessary cleanup. It must be called to balance every successful SPI_connect call to prevent memory leaks and maintain proper SPI stack state.

The function performs the following cleanup operations:
1. **Connection Validation**: Calls _SPI_begin_call(false) to verify an active SPI connection exists
2. **Memory Context Restoration**: Switches back to the saved memory context from before the SPI connection
3. **Memory Cleanup**: Deletes both execCxt and procCxt memory contexts, freeing all memory allocated during SPI operations
4. **Global Variable Restoration**: Restores the outer SPI state (SPI_processed, SPI_tuptable, SPI_result) from the saved values
5. **Stack Management**: Decrements _SPI_connected to exit the current stack level and updates _SPI_current pointer

The function ensures that nested SPI connections are properly unwound and that any memory allocated during SPI operations (including tuple tables) is freed.

## Parameters / Member Variables
This function takes no parameters and operates on the current SPI connection state.

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call (connection validation)
  - MemoryContextSwitchTo (context restoration)
  - MemoryContextDelete (memory cleanup)

- Called from (representative examples):
  - refresh_by_match_merge (materialized view operations)
  - ri_Check_Pk_Match, ri_restrict, RI_FKey_cascade_del (foreign key constraint handling)
  - pg_get_ruledef_worker, pg_get_viewdef_worker (rule and view definition functions)
  - plperl_func_handler, plperl_trigger_handler (Perl procedural language)
  - PLy_exec_function, PLy_exec_trigger (Python procedural language)
  - pltcl_func_handler, pltcl_trigger_handler (Tcl procedural language)
  - Various XML processing functions

## Notes and Other Information
- Must be called to balance every successful SPI_connect/SPI_connect_ext call
- Properly handles nested SPI connections by maintaining stack-based state
- Deletes all memory contexts created during the SPI connection, preventing memory leaks
- Restores global SPI variables to their state before the connection was established
- Returns SPI_OK_FINISH (2) on successful completion
- Returns SPI_ERROR_UNCONNECTED if called without an active SPI connection
- After calling SPI_finish, any tuple tables or other data from the finished connection become invalid
- Located in src/backend/executor/spi.c:182-221