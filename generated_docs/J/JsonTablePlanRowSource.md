# JsonTablePlanRowSource

## Location
[src/backend/utils/adt/jsonpath_exec.c:169-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L169-L173)

## Overview
JsonTablePlanRowSource is a structure that holds the result of jsonpath evaluation, serving as a source row for JsonTableGetValue() which computes the values of individual JSON_TABLE columns.

## Definition

```c
typedef struct JsonTablePlanRowSource
{
	Datum		value;
	bool		isnull;
} JsonTablePlanRowSource;
```
## Detailed Description
JsonTablePlanRowSource is a fundamental data structure used in PostgreSQL's JSON_TABLE functionality. It acts as an intermediate container that holds the evaluated result from a jsonpath expression. This structure bridges the gap between jsonpath evaluation and the computation of individual JSON_TABLE column values. The structure is designed to encapsulate both the actual data value and its nullability status, which is crucial for proper SQL NULL handling in JSON operations.

## Parameters / Member Variables
- `value`: A Datum containing the evaluated result from jsonpath execution
- `isnull`: A boolean flag indicating whether the value is NULL
## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls from this structure definition)
- Called from (representative examples):
  - [JsonTablePlanState](JsonTablePlanState.md) (contains this as a member)
  - [JsonTableGetValue](JsonTableGetValue.md) (uses this structure for column value computation)

## Notes and Other Information
- This structure is part of PostgreSQL's JSON_TABLE execution framework
- Located in src/backend/utils/adt/jsonpath_exec.c at lines 169-173
- The structure follows PostgreSQL's standard pattern of pairing a Datum value with a NULL indicator
- It serves as a critical component in the JSON_TABLE row processing pipeline