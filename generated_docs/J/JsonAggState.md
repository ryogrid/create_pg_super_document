# JsonAggState

## Location
[src/backend/utils/adt/json.c:76-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L76-L84)

## Overview
JsonAggState is a state structure used during JSON aggregation operations in PostgreSQL, maintaining the accumulated state and metadata for both json_agg() and json_object_agg() aggregate functions.

## Definition

```c
typedef struct JsonAggState
{
	StringInfo	str;
	JsonTypeCategory key_category;
	Oid			key_output_func;
	JsonTypeCategory val_category;
	Oid			val_output_func;
	JsonUniqueBuilderState unique_check;
} JsonAggState;
```
## Detailed Description
JsonAggState serves as the accumulator state for PostgreSQL's JSON aggregation functions. It maintains a string buffer that builds up the JSON output incrementally as the aggregation progresses. The structure stores type categorization information and output function OIDs for both keys and values, enabling proper conversion of different PostgreSQL data types to their JSON representations. Additionally, it includes a unique checking mechanism to handle duplicate key detection in json_object_agg operations.

## Parameters / Member Variables
- `str`: StringInfo buffer that accumulates the JSON output string as aggregation progresses
- `key_category`: JsonTypeCategory enum value categorizing the data type of keys (used in json_object_agg)
- `key_output_func`: OID of the output function used to convert key values to their string representation
- `val_category`: JsonTypeCategory enum value categorizing the data type of values being aggregated
- `val_output_func`: OID of the output function used to convert values to their string representation
- `unique_check`: JsonUniqueBuilderState structure for tracking and detecting duplicate keys in object aggregation
## Dependencies
- Functions called/Symbols referenced:
  - JsonTypeCategory
  - [JsonUniqueBuilderState](JsonUniqueBuilderState.md)
  - StringInfo
  - Oid
- Called from (representative examples):
  - [json_agg_transfn_worker](../j/json_agg_transfn_worker.md)
  - [json_agg_finalfn](../j/json_agg_finalfn.md)
  - [json_object_agg_transfn_worker](../j/json_object_agg_transfn_worker.md)
  - [json_object_agg_finalfn](../j/json_object_agg_finalfn.md)

## Notes and Other Information
- This structure is primarily used internally by the PostgreSQL JSON aggregation system and is not exposed to SQL users directly
- The state is maintained across multiple calls to transition functions during aggregation
- The unique_check member is particularly important for json_object_agg to ensure object keys are unique
- Located in src/backend/utils/adt/json.c at lines 76-84
- Works in conjunction with PostgreSQL's aggregate function framework to provide efficient JSON construction