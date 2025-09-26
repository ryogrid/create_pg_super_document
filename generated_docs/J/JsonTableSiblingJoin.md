# JsonTableSiblingJoin

## Location
[src/include/nodes/primnodes.h:1923-1929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1923-L1929)

## Overview
JsonTableSiblingJoin is a JSON_TABLE plan type that joins rows from sibling NESTED COLUMNS clauses within the same parent COLUMNS clause.

## Definition
```c
typedef struct JsonTableSiblingJoin
{
    JsonTablePlan plan;
    JsonTablePlan *lplan;
    JsonTablePlan *rplan;
} JsonTableSiblingJoin;
```

## Detailed Description
JsonTableSiblingJoin extends JsonTablePlan to implement a join strategy for combining results from sibling nested column specifications. It coordinates the execution of two child plans (left and right) that operate on the same hierarchical level within a JSON_TABLE structure, ensuring proper row correlation and data combination for complex nested JSON_TABLE queries.

## Parameters / Member Variables
- `plan`: Base JsonTablePlan structure providing common plan functionality
- `lplan`: Left child JsonTablePlan for the first sibling nested columns clause
- `rplan`: Right child JsonTablePlan for the second sibling nested columns clause

## Dependencies
- Functions called/Symbols referenced:
  - [JsonTablePlan](JsonTablePlan.md)
- Called from (representative examples):
  - [makeJsonTableSiblingJoin](../m/makeJsonTableSiblingJoin.md)
  - [JsonTableInitPlan](JsonTableInitPlan.md)
  - [JsonTablePlanNextRow](JsonTablePlanNextRow.md)
  - [JsonTableResetNestedPlan](JsonTableResetNestedPlan.md)
  - [get_json_table_nested_columns](../g/get_json_table_nested_columns.md)

## Notes and Other Information
- Concrete implementation of the abstract JsonTablePlan base class
- Specifically designed to handle sibling relationships in nested JSON_TABLE structures
- Coordinates execution between two child plans to produce joined results
- Essential for complex JSON_TABLE queries with multiple nested column groups at the same level
- Part of the hierarchical execution strategy for JSON_TABLE operations
- Enables PostgreSQL to handle sophisticated JSON data extraction patterns as specified in the SQL/JSON standard