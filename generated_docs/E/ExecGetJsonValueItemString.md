# ExecGetJsonValueItemString

## Location
src/backend/executor/execExprInterp.c: 4481 - 4555

## Overview
Converts a JsonbValue to its C string representation for use in JSON_VALUE operations, handling all JSONB scalar and composite types.

## Definition
```c
static char *ExecGetJsonValueItemString(JsonbValue *item, bool *resnull)
```

## Detailed Description
This static function provides type-aware conversion of JsonbValue structures to C strings, supporting the full range of JSONB types including scalars (null, string, numeric, boolean), datetime types (date, time, timestamp variants), and composite types (arrays, objects, binary). It handles proper memory allocation for string values and delegates to appropriate output functions for each data type. The function sets the resnull flag for null values and returns properly formatted string representations suitable for further processing or coercion.

## Parameters / Member Variables
- `item`: JsonbValue pointer containing the value to convert
- `resnull`: Pointer to boolean flag that will be set to true if the value is null

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - memcpy
  - [DatumGetCString](../D/DatumGetCString.md)
  - DirectFunctionCall1
  - [numeric_out](../n/numeric_out.md)
  - [NumericGetDatum](../N/NumericGetDatum.md)
  - [boolout](../b/boolout.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [date_out](../d/date_out.md), time_out, timetz_out, timestamp_out, timestamptz_out
  - [jsonb_out](../j/jsonb_out.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - [JsonbPGetDatum](../J/JsonbPGetDatum.md)
- Called from (representative examples):
  - [ExecEvalJsonExprPath](ExecEvalJsonExprPath.md)
  - EEO_JUMP (expression evaluation optimization)

## Notes and Other Information
- Returns NULL and sets *resnull=true for jbvNull values
- For jbvString, performs manual memory allocation and copying to ensure null termination
- Handles all datetime types (DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ) with appropriate output functions
- Complex types (arrays, objects, binary) are converted back to JSONB string representation
- Used primarily in JSON_VALUE operations where scalar extraction requires string conversion
- Static function scope limits its use to the execExprInterp.c compilation unit
- Memory allocated by this function should be managed by PostgreSQL's memory context system
- Essential for type coercion pipeline in SQL/JSON value extraction