# initVacuum

## Location
[src/bin/pgbench/pgbench.c:5162-5174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5162-L5174)

## Overview
The  function performs vacuum and analyze operations on all pgbench tables to optimize database performance after data initialization.

## Definition


## Detailed Description
This function executes VACUUM ANALYZE commands on all four standard pgbench tables to reclaim storage space and update table statistics after bulk data loading. The VACUUM operation removes dead tuples and compacts table storage, while ANALYZE updates the query planner's statistics about data distribution. This step is crucial after large data loads to ensure optimal query performance during benchmark execution. The function processes all pgbench tables systematically: branches, tellers, accounts, and history tables.

## Parameters / Member Variables
- : Active PostgreSQL database connection handle used for executing the vacuum operations

## Dependencies
- Functions called/Symbols referenced:
  - [executeStatement](../e/executeStatement.md): Executes VACUUM ANALYZE SQL commands for each table
- Called from (representative examples):
  - [runInitSteps](../r/runInitSteps.md): Main initialization workflow that invokes vacuum as a final optimization step

## Notes and Other Information
- Executes VACUUM ANALYZE rather than just VACUUM to both reclaim space and update statistics
- Processes all four pgbench tables: pgbench_branches, pgbench_tellers, pgbench_accounts, and pgbench_history
- Essential for optimal benchmark performance after bulk data loading operations
- The history table is included even though it may be empty after initialization
- Provides user feedback by printing a "vacuuming..." message to stderr
- Part of the standard pgbench initialization workflow, typically run after data generation
- VACUUM ANALYZE combination ensures both storage optimization and accurate query planning statistics
- Static function used only within the pgbench initialization process