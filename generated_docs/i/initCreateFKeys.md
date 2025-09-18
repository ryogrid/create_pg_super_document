# initCreateFKeys

## Location
[src/bin/pgbench/pgbench.c:5213-5238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5213-L5238)

## Overview
Creates foreign key constraints between the standard pgbench tables to establish referential integrity relationships.

## Definition
```c
static void initCreateFKeys(PGconn *con)
```

## Detailed Description
This function is part of pgbench's database initialization process and is responsible for adding foreign key constraints between the four standard pgbench tables. It creates the referential integrity relationships that model a simple banking scenario where branches contain tellers and accounts, and history records track transactions involving specific branches, tellers, and accounts.

The function executes a series of ALTER TABLE statements that establish the following foreign key relationships:
- pgbench_tellers.bid → pgbench_branches.bid
- pgbench_accounts.bid → pgbench_branches.bid  
- pgbench_history.bid → pgbench_branches.bid
- pgbench_history.tid → pgbench_tellers.tid
- pgbench_history.aid → pgbench_accounts.aid

This creates a hierarchical data model where branches are at the top level, tellers and accounts belong to branches, and history records reference all three entity types.

## Parameters / Member Variables
- `con`: PostgreSQL connection handle used to execute the DDL statements

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro to get array length)
  - [executeStatement](../e/executeStatement.md) (execute SQL statement)
- Called from (representative examples):
  - [runInitSteps](../r/runInitSteps.md)

## Notes and Other Information
- This function is called after primary keys have been created (via initCreatePKeys) since foreign keys require the referenced primary keys to exist first
- The foreign key constraints enforce referential integrity and prevent orphaned records in the pgbench tables
- Unlike initCreatePKeys, this function does not support tablespace specification since foreign key constraints are metadata only
- The constraint names follow a consistent pattern: [table_name]_[column_name]_fkey
- Located in src/bin/pgbench/pgbench.c:5213-5238