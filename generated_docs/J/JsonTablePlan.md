# JsonTablePlan

## Location
src/include/nodes/primnodes.h: 1882 - 1887

## Overview
JsonTablePlan is an abstract base class representing different types of JSON_TABLE execution plans used to generate row patterns by evaluating JSON path expressions.

## Definition
```c
typedef struct JsonTablePlan
{
    pg_node_attr(abstract)
    NodeTag     type;
} JsonTablePlan;
```

## Detailed Description
JsonTablePlan serves as an abstract base class for various JSON_TABLE plan types. It provides the foundation for implementing different strategies to generate "row pattern" values by evaluating JSON path expressions against input JSON documents. These row patterns are then used to populate the columns of JSON_TABLE() results. The abstract nature allows for different concrete implementations like JsonTablePathScan and JsonTableSiblingJoin.

## Parameters / Member Variables
- `type`: NodeTag identifying the specific concrete type of JSON_TABLE plan

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (no direct references from this symbol)
- Called from (representative examples):
  - generateJsonTablePathName
  - transformJsonTableColumns
  - transformJsonTableColumn
  - makeJsonTablePathScan
  - makeJsonTableSiblingJoin

## Notes and Other Information
- Abstract base class - not instantiated directly
- Concrete implementations include JsonTablePathScan and JsonTableSiblingJoin
- Central to the JSON_TABLE execution strategy in PostgreSQL
- Part of the SQL/JSON standard implementation for tabular JSON data extraction
- The pg_node_attr(abstract) annotation indicates this is meant to be subclassed
- Used throughout the JSON_TABLE parsing and execution pipeline