# get_stats_option_name

## Location
src/backend/tcop/postgres.c: 3837 - 3876

## Overview
A utility function that maps single-character command-line option arguments to their corresponding PostgreSQL statistics logging GUC parameter names.

## Definition


## Detailed Description
This function serves as a mapping utility for PostgreSQL's statistics logging command-line options. It takes a command-line argument string and returns the corresponding GUC (Grand Unified Configuration) parameter name for statistics logging. The function specifically handles three types of statistics logging options:

- Parser statistics (triggered by 'pa' argument)
- Planner statistics (triggered by 'pl' argument)  
- Executor statistics (triggered by 'e' argument)

The function uses a simple switch statement on the first character of the argument, with additional character checking for disambiguation between parser and planner options.

## Parameters / Member Variables
- : A string containing the command-line argument that specifies which statistics option to enable. Expected values are "pa" for parser, "pl" for planner, or "e" for executor.

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls
- Called from (representative examples):
  - PostmasterMain (in src/backend/postmaster/postmaster.c:726)
  - process_postgres_switches (in src/backend/tcop/postgres.c:4048)

## Notes and Other Information
- Returns NULL if the argument doesn't match any recognized statistics option
- The function assumes the argument string is at least one character long
- Part of PostgreSQL's command-line argument processing infrastructure
- Used during server startup to enable specific statistics logging based on command-line flags
- The returned string corresponds to actual GUC parameter names that can be set in postgresql.conf