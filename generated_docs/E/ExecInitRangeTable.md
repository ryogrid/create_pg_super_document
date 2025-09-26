# ExecInitRangeTable

## Location
[src/backend/executor/execUtils.c:728-761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L728-L761)

## Overview
Initializes the executor's range table data structures, setting up arrays and storage for managing relations during query execution.

## Definition
```c
void ExecInitRangeTable(EState *estate, List *rangeTable, List *permInfos)
```

## Detailed Description
ExecInitRangeTable is a crucial initialization function that sets up the executor's range table infrastructure. The range table is a fundamental data structure in PostgreSQL that tracks all relations (tables, views, subqueries, etc.) referenced in a query.

This function performs several key initialization tasks:

1. Stores the range table list and permission information list in the execution state
2. Calculates and stores the range table size for array allocation
3. Allocates an array of Relation pointers (es_relations) parallel to the range table, initialized to NULL. Relations are opened lazily as needed during execution
4. Initializes other parallel arrays (es_result_relations and es_rowmarks) to NULL, as these are allocated only when needed

The lazy initialization approach for relation opening helps optimize query startup time by only opening relations when they are actually accessed during execution.

## Parameters / Member Variables
- `estate`: Execution state structure that will store the range table information and associated arrays
- `rangeTable`: List of RangeTblEntry structures representing all relations referenced in the query
- `permInfos`: List of RTEPermissionInfo structures containing permission information for the range table entries

## Dependencies
- Functions called/Symbols referenced:
  - list_length
  - palloc0
- Called from (representative examples):
  - CopyFrom
  - InitPlan
  - create_edata_for_relation
  - create_estate_for_relation

## Notes and Other Information
- This function is typically called during query initialization before execution begins
- The es_relations array uses lazy initialization - relations are opened only when needed
- Located in src/backend/executor/execUtils.c:728-761
- Essential for setting up the executor's relation management infrastructure
- The parallel arrays (es_relations, es_result_relations, es_rowmarks) are all indexed by range table index
- Memory allocation uses palloc0 to ensure arrays are zero-initialized
- The function supports both regular query execution and specialized contexts like COPY and logical replication