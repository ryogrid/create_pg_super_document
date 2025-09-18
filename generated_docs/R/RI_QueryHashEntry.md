# RI_QueryHashEntry

## Location
src/backend/utils/adt/ri_triggers.c: 141 - 145

## Overview
RI_QueryHashEntry is a hash table entry structure that stores prepared SPI query plans for referential integrity operations, combining a query key with its corresponding prepared plan for efficient caching and retrieval.

## Definition


## Detailed Description
RI_QueryHashEntry represents an entry in the referential integrity query plan cache hash table. Each entry associates a specific query key (identifying a constraint and operation type) with its corresponding prepared SPI plan. This structure enables PostgreSQL to cache and reuse SQL execution plans for foreign key constraint operations, significantly improving performance by avoiding the overhead of repeatedly parsing and planning the same types of queries.

## Parameters / Member Variables
- : RI_QueryKey structure that uniquely identifies the constraint and query type for this cached plan
- : SPIPlanPtr pointing to the prepared SPI plan that can be executed for this specific referential integrity operation

## Dependencies
- Functions called/Symbols referenced:
  - [RI_QueryKey](RI_QueryKey.md)
  - [SPIPlanPtr](../S/SPIPlanPtr.md)
- Called from (representative examples):
  - [ri_InitHashTables](../r/ri_InitHashTables.md)
  - [ri_FetchPreparedPlan](../r/ri_FetchPreparedPlan.md)
  - [ri_HashPreparedPlan](../r/ri_HashPreparedPlan.md)

## Notes and Other Information
This structure is central to PostgreSQL's query plan caching mechanism for referential integrity operations. The hash table using these entries is initialized by ri_InitHashTables and is used throughout the referential integrity system to store and retrieve prepared plans for different types of constraint checking operations. The SPIPlanPtr allows the system to execute the cached plans directly without re-parsing or re-planning the SQL statements, providing significant performance benefits for applications with frequent foreign key constraint operations.