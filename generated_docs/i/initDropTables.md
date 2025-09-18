# initDropTables

## Location
src/bin/pgbench/pgbench.c: 4732 - 4753

## Overview
Removes existing pgbench tables from the database before initializing new ones, ensuring a clean state for benchmark operations.

## Definition


## Detailed Description
The initDropTables function is a cleanup utility in pgbench that removes all standard pgbench tables (pgbench_accounts, pgbench_branches, pgbench_history, and pgbench_tellers) from the database. It uses a single DROP TABLE statement with the IF EXISTS clause to safely remove tables regardless of whether they exist or not. The function drops all tables in one command to handle potential foreign key dependencies gracefully, avoiding dependency-related errors that could occur if tables were dropped individually.

## Parameters / Member Variables
- `con`: PGconn pointer to the PostgreSQL database connection used to execute the drop statement

## Dependencies
- Functions called/Symbols referenced:
  - [executeStatement](../e/executeStatement.md) (for executing the SQL drop command)
- Called from (representative examples):
  - [runInitSteps](../r/runInitSteps.md) (part of the pgbench initialization process)

## Notes and Other Information
- Uses IF EXISTS to prevent errors when tables don't exist
- Drops all pgbench tables in a single statement to handle foreign key dependencies
- Part of the pgbench initialization sequence, ensuring clean database state
- Outputs progress message to stderr to inform users of the cleanup operation
- Static function scope limits its usage to within pgbench.c