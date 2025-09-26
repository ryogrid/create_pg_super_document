# JsonTablePathScan

## Location
src/include/nodes/primnodes.h: 1893 - 1916

## Overview
JsonTablePathScan is a concrete JSON_TABLE plan type that evaluates a JSON path expression and handles nested paths for JSON_TABLE operations.

## Definition
```c
typedef struct JsonTablePathScan
{
    JsonTablePlan plan;
    JsonTablePath *path;
    bool          errorOnError;
    JsonTablePlan *child;
    int           colMin;
    int           colMax;
} JsonTablePathScan;
```

## Detailed Description
JsonTablePathScan extends JsonTablePlan to implement a specific strategy for scanning JSON documents using path expressions. It evaluates a JSON path expression against input JSON data and can handle nested column structures through child plans. The structure manages column ranges, error handling behaviors, and hierarchical path evaluation for complex JSON_TABLE queries.

## Parameters / Member Variables
- `plan`: Base JsonTablePlan structure for plan node functionality
- `path`: JsonTablePath containing the JSON path expression to evaluate
- `errorOnError`: Boolean flag for ERROR/EMPTY ON ERROR behavior (significant only for top-level paths)
- `child`: Nested JsonTablePlan for handling nested columns, if any exist
- `colMin`: 0-based index of the first column covered by this plan (-1 if all columns are nested)
- `colMax`: 0-based index of the last column covered by this plan (-1 if all columns are nested)

## Dependencies
- Functions called/Symbols referenced:
  - JsonTablePlan
  - JsonTablePath
- Called from (representative examples):
  - makeJsonTablePathScan
  - JsonTableInitPlan
  - JsonTableResetRowPattern
  - JsonTablePlanNextRow
  - get_json_table_nested_columns

## Notes and Other Information
- Concrete implementation of the abstract JsonTablePlan base class
- Handles both simple path evaluation and complex nested column structures
- Column range tracking (colMin/colMax) enables efficient column processing
- Error handling behavior can be customized through the errorOnError flag
- Supports hierarchical JSON_TABLE operations through child plan chaining
- Central to PostgreSQL's SQL/JSON JSON_TABLE functionality implementation