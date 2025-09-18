# pgstat_create_function

## Location
[src/backend/utils/activity/pgstat_function.c:45-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_function.c#L45-L59)

## Overview
Registers a new function in PostgreSQL's statistics tracking system when the function is created, ensuring proper transaction-aware statistics management.

## Definition


## Detailed Description
This function registers a new function (identified by its OID) with PostgreSQL's statistics tracking subsystem. It acts as a thin wrapper around the more general  function, specifically handling function-type statistics objects. The function ensures that if the current transaction aborts, any statistics tracking for this function will be properly cleaned up. This is part of PostgreSQL's transactional statistics system where statistics operations are tied to transaction outcomes.

## Parameters / Member Variables
- : The object identifier (OID) of the function being registered for statistics tracking

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_create_transactional
  - PGSTAT_KIND_FUNCTION
  - MyDatabaseId
- Called from (representative examples):
  - [ProcedureCreate](../P/ProcedureCreate.md) (in src/backend/catalog/pg_proc.c:711)

## Notes and Other Information
- This function is called during function creation to ensure proper statistics tracking
- The statistics registration is transactional - if the transaction creating the function is rolled back, the statistics entry will also be cleaned up
- Located in src/backend/utils/activity/pgstat_function.c:45-59
- Part of PostgreSQL's comprehensive statistics collection framework for monitoring database performance