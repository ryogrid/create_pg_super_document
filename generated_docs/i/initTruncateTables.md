# initTruncateTables

## Location
[src/bin/pgbench/pgbench.c:4918-4927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4918-L4927)

## Overview
Truncates all existing data from pgbench tables to prepare them for fresh data generation during benchmark initialization.

## Definition
```c
static void initTruncateTables(PGconn *con)
```

## Detailed Description
The initTruncateTables function removes all existing data from the four standard pgbench tables (pgbench_accounts, pgbench_branches, pgbench_history, and pgbench_tellers) using a single TRUNCATE statement. This approach is more efficient than DELETE for removing all rows and handles foreign key relationships properly by truncating all related tables in one atomic operation. The function is used when reinitializing pgbench data without recreating the table structures.

## Parameters / Member Variables
- `con`: PGconn pointer to the PostgreSQL database connection used to execute the truncate statement

## Dependencies
- Functions called/Symbols referenced:
  - [executeStatement](../e/executeStatement.md) (for executing the SQL truncate command)
- Called from (representative examples):
  - [initGenerateDataClientSide](initGenerateDataClientSide.md) (before generating data on client side)
  - [initGenerateDataServerSide](initGenerateDataServerSide.md) (before generating data on server side)

## Notes and Other Information
- Uses a single TRUNCATE statement to handle all tables simultaneously
- More efficient than DELETE for removing all rows as it doesn't scan the table
- Handles foreign key dependencies by truncating all related tables together
- Preserves table structure and indexes while removing all data
- Static function scope limits its usage to within pgbench.c
- Part of the data generation initialization process in pgbench