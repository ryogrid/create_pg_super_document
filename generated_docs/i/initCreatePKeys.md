# initCreatePKeys

## Location
[src/bin/pgbench/pgbench.c:5175-5212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5175-L5212)

## Overview
Creates primary keys on the standard pgbench tables (pgbench_branches, pgbench_tellers, and pgbench_accounts) during database initialization.

## Definition
```c
static void initCreatePKeys(PGconn *con)
```

## Detailed Description
This function is part of pgbench's database initialization process and is responsible for adding primary key constraints to the three standard pgbench tables. It executes ALTER TABLE statements to add primary keys on the appropriate ID columns (bid for branches, tid for tellers, aid for accounts). The function supports an optional index tablespace specification that can be applied to the primary key indexes.

The function iterates through a predefined array of DDL statements and executes each one using the executeStatement utility function. If an index tablespace is specified via the global index_tablespace variable, it appends the appropriate USING INDEX TABLESPACE clause to each ALTER TABLE statement.

## Parameters / Member Variables
- `con`: PostgreSQL connection handle used to execute the DDL statements

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (buffer structure for query building)
  - [initPQExpBuffer](initPQExpBuffer.md) (initialize query buffer)
  - lengthof (macro to get array length)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md) (reset query buffer)
  - [PQescapeIdentifier](../P/PQescapeIdentifier.md) (escape tablespace identifier)
  - [PQfreemem](../P/PQfreemem.md) (free PostgreSQL allocated memory)
  - [executeStatement](../e/executeStatement.md) (execute SQL statement)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup query buffer)
- Called from (representative examples):
  - [runInitSteps](../r/runInitSteps.md)

## Notes and Other Information
- This function is called as part of the pgbench initialization sequence after tables are created but before foreign keys are added
- The primary keys created are: bid on pgbench_branches, tid on pgbench_tellers, and aid on pgbench_accounts
- Supports optional index tablespace specification for performance tuning
- Uses proper identifier escaping when handling tablespace names to prevent SQL injection
- Located in src/bin/pgbench/pgbench.c:5175-5212