# json_agg_transfn_worker

## Location
src/backend/utils/adt/json.c: 770 - 851

## Overview
The json_agg_transfn_worker function implements the core transition logic for PostgreSQL's json_agg aggregate function, building a JSON array from input values.

## Definition
```c
static Datum json_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null)
```

## Detailed Description
This function serves as the workhorse for JSON aggregation operations, implementing the state transition logic that accumulates input values into a JSON array format. It manages a JsonAggState structure that maintains the growing JSON array string and type information. The function handles initialization of the aggregate state, proper comma separation between array elements, null value processing, and formatting for complex types. It supports both regular and strict modes via the absent_on_null parameter, where strict mode skips null values entirely.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo containing the aggregate context and arguments
- `absent_on_null`: Boolean flag controlling null handling behavior (true for strict mode)

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext (to validate aggregate execution context)
  - get_fn_expr_argtype (to determine input data type)
  - MemoryContextSwitchTo (for memory management in aggregate context)
  - makeStringInfo (to create the JSON array buffer)
  - json_categorize_type (to categorize input type for JSON conversion)
  - datum_to_json_internal (to convert individual values to JSON)
- Called from:
  - json_agg_transfn (standard json_agg aggregate function)
  - json_agg_strict_transfn (strict variant that skips nulls)

## Notes and Other Information
- Maintains state across aggregate calls using JsonAggState structure
- Handles memory context switching to ensure state persists for the aggregate duration
- Adds proper formatting including commas between elements and whitespace for structured types
- Located in src/backend/utils/adt/json.c:770-851
- Uses 'internal' transition type for efficient state passing through PostgreSQL's aggregate machinery
- Supports both null-preserving and null-skipping modes of operation