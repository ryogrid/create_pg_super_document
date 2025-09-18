# initGenerateDataClientSide

## Location
[src/bin/pgbench/pgbench.c:5085-5116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5085-L5116)

## Overview
The  function orchestrates the client-side generation and insertion of test data into all pgbench standard tables within a single database transaction.

## Definition


## Detailed Description
This function implements the client-side data generation strategy for pgbench database initialization. It generates test data on the client side and sends it to the PostgreSQL server using the COPY protocol for efficient bulk loading. The function ensures data consistency by wrapping all operations in a single transaction and populates tables in the correct order to respect potential foreign key constraints. It handles the complete initialization workflow: truncating existing data, generating data for branches, tellers, and accounts tables in proper sequence.

## Parameters / Member Variables
- : Active PostgreSQL database connection handle used for all database operations

## Dependencies
- Functions called/Symbols referenced:
  - [executeStatement](../e/executeStatement.md): Executes BEGIN and COMMIT transaction statements
  - [initTruncateTables](initTruncateTables.md): Truncates all pgbench tables to remove old data
  - [initPopulateTable](initPopulateTable.md): Core function for populating individual tables with generated data
  - [initBranch](initBranch.md): Row generation function for pgbench_branches table
  - [initTeller](initTeller.md): Row generation function for pgbench_tellers table  
  - [initAccount](initAccount.md): Row generation function for pgbench_accounts table
  - nbranches, ntellers, naccounts: Global variables specifying record counts per table
- Called from (representative examples):
  - [runInitSteps](../r/runInitSteps.md): Main initialization workflow that invokes this function for client-side data generation

## Notes and Other Information
- All operations are performed within a single transaction to enable PostgreSQL's data-loading optimizations
- Tables are populated in a specific order (branches, tellers, accounts) to handle potential foreign key relationships
- Uses client-side data generation as opposed to server-side generation methods
- The filler column behavior varies by table: NULL for branches/tellers, blank-padded for accounts
- Provides user feedback by printing progress messages to stderr
- Automatically truncates existing data before generating new data
- Part of pgbench's modular initialization system that supports different data generation strategies