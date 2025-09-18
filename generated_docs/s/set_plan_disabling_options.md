# set_plan_disabling_options

## Location
src/backend/tcop/postgres.c: 3795 - 3836

## Overview
A utility function that disables specific query execution plan strategies based on single-character command line arguments, used for testing and debugging query optimization behavior.

## Definition


## Detailed Description
This function provides a convenient mechanism to disable various query execution strategies through single-character command line arguments. It maps each character to a corresponding "enable_*" GUC parameter and sets it to "false", effectively disabling that particular execution strategy. This functionality is primarily used for testing the query planner's behavior when certain execution methods are unavailable, debugging performance issues, or forcing the planner to choose alternative execution paths.

## Parameters / Member Variables
- : A string containing single-character codes representing execution strategies to disable ('s' for seqscan, 'i' for indexscan, etc.)
- : The GUC context indicating when/how this configuration change is being applied
- : The source of the configuration change (e.g., command line, configuration file, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [SetConfigOption](../S/SetConfigOption.md): Sets individual GUC parameters programmatically to disable execution strategies
  - GucContext: Context type for GUC operations
  - GucSource: Source type for GUC operations
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md): Processes command-line options during postmaster startup
  - [process_postgres_switches](../p/process_postgres_switches.md): Processes various PostgreSQL command-line switches

## Notes and Other Information
- Supported single-character codes and their corresponding strategies:
  - 's': disable sequential scans (enable_seqscan)
  - 'i': disable index scans (enable_indexscan)  
  - 'o': disable index-only scans (enable_indexonlyscan)
  - 'b': disable bitmap scans (enable_bitmapscan)
  - 't': disable TID scans (enable_tidscan)
  - 'n': disable nested loop joins (enable_nestloop)
  - 'm': disable merge joins (enable_mergejoin)
  - 'h': disable hash joins (enable_hashjoin)
- Returns true if the character was recognized and the corresponding option was disabled
- Returns false if the character is not recognized
- Primarily used for debugging and testing query planner behavior under constrained conditions