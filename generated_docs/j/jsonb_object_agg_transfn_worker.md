# jsonb_object_agg_transfn_worker

## Location
src/backend/utils/adt/jsonb.c: 1673 - 1895

## Overview
Core worker function that implements the transition logic for JSONB object aggregation, handling key-value pair accumulation with configurable null handling and key uniqueness policies.

## Definition


## Detailed Description
This is the comprehensive worker function that powers all JSONB object aggregation variants. It accumulates key-value pairs into a JSONB object during aggregate processing. The function handles initialization of the aggregate state on first call, validates key types (must be strings), processes both keys and values through the JSONB conversion pipeline, and maintains the growing object structure. It supports configurable behavior for NULL value handling and key uniqueness validation through its boolean parameters.

The function operates in two main phases: state initialization (first call) where it sets up JsonbAggState and determines input data types, and accumulation phase where it processes each key-value pair. Keys must be convertible to strings, while values can be any type including complex nested structures.

## Parameters / Member Variables
- : Function call information containing aggregate state and input key/value arguments
- : If true, skip key-value pairs where the value is NULL (unless unique_keys is true)
- : If true, enforce unique key constraints in the resulting object

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbInState](../J/JsonbInState.md), JsonbAggState, JsonbIterator, Jsonb
  - [AggCheckCallContext](../A/AggCheckCallContext.md), MemoryContextSwitchTo
  - [pushJsonbValue](../p/pushJsonbValue.md), JsonbIteratorInit, JsonbIteratorNext
  - datum_to_jsonb_internal, JsonbValueToJsonb
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md), json_categorize_type
  - WJB_BEGIN_OBJECT, WJB_KEY, WJB_VALUE, WJB_END_ARRAY, etc.
- Called from (representative examples):
  - [jsonb_object_agg_transfn](jsonb_object_agg_transfn.md)
  - [jsonb_object_agg_strict_transfn](jsonb_object_agg_strict_transfn.md)
  - [jsonb_object_agg_unique_transfn](jsonb_object_agg_unique_transfn.md)
  - [jsonb_object_agg_unique_strict_transfn](jsonb_object_agg_unique_strict_transfn.md)

## Notes and Other Information
- Static function, only accessible within the same compilation unit
- Enforces that object keys must be strings, raising errors for other types
- Handles memory context switching for proper aggregate context management
- Supports both simple scalar values and complex nested JSON structures as values
- Implements string and numeric value copying to ensure proper memory management
- Part of PostgreSQL's JSONB aggregate function family located in src/backend/utils/adt/jsonb.c:1673-1895