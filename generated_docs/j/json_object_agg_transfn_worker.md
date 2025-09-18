# json_object_agg_transfn_worker

## Location
[src/backend/utils/adt/json.c:993-1140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L993-L1140)

## Overview
This function is the core worker implementation for the json_object_agg PostgreSQL aggregate function, building JSON objects by accumulating key-value pairs with optional duplicate key checking and null value handling.

## Definition
```c
static Datum json_object_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null, bool unique_keys)
```

## Detailed Description
The function implements the transition logic for PostgreSQL's json_object_agg aggregate function. It accumulates input key-value pairs into a JSON object representation stored in a StringInfo buffer. The function handles several important aspects:

1. **State Management**: On first call, initializes JsonAggState containing the output buffer and optional unique key checking infrastructure
2. **Type Resolution**: Determines output functions for both key and value data types using json_categorize_type
3. **Duplicate Key Detection**: When unique_keys is enabled, maintains a hash table to detect and reject duplicate keys
4. **Null Value Handling**: When absent_on_null is true, skips entries with null values entirely
5. **JSON Formatting**: Properly formats output with comma delimiters and colon separators

The function operates within PostgreSQL's aggregate framework and ensures proper memory context management for persistent state across aggregate calls.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information containing arguments and context
- `absent_on_null`: Boolean flag indicating whether to skip entries with null values
- `unique_keys`: Boolean flag indicating whether to enforce key uniqueness

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - makeStringInfo
  - [json_unique_builder_init](json_unique_builder_init.md)
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - [json_categorize_type](json_categorize_type.md)
  - [json_unique_builder_get_throwawaybuf](json_unique_builder_get_throwawaybuf.md)
  - [datum_to_json_internal](../d/datum_to_json_internal.md)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [json_unique_check_key](json_unique_check_key.md)
- Data structures used:
  - [JsonAggState](../J/JsonAggState.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - StringInfo
  - [MemoryContext](../M/MemoryContext.md)
- Called from (representative examples):
  - [json_object_agg_transfn](json_object_agg_transfn.md)
  - [json_object_agg_strict_transfn](json_object_agg_strict_transfn.md)
  - [json_object_agg_unique_transfn](json_object_agg_unique_transfn.md)
  - [json_object_agg_unique_strict_transfn](json_object_agg_unique_strict_transfn.md)

## Notes and Other Information
- This is a static function, only accessible within the json.c compilation unit
- Handles PostgreSQL's "any" type arguments, including UNKNOWN types
- Enforces non-null keys as required by JSON specification
- Uses memory context switching to ensure aggregate state persists across calls
- Implements performance optimizations for null value skipping and duplicate key detection
- Returns ERROR for duplicate keys when unique_keys is enabled
- Part of PostgreSQL's SQL standard JSON aggregate function implementation