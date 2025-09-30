# ExecGetJsonValueItemString

## Location
[src/backend/executor/execExprInterp.c:4481-4555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4481-L4555)

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
- [Complex](../C/Complex.md) types (arrays, objects, binary) are converted back to JSONB string representation
- Used primarily in JSON_VALUE operations where scalar extraction requires string conversion
- Static function scope limits its use to the execExprInterp.c compilation unit
- Memory allocated by this function should be managed by PostgreSQL's memory context system
- Essential for type coercion pipeline in SQL/JSON value extraction

## Simplified Source

```c
static char *
ExecGetJsonValueItemString(JsonbValue *item, bool *resnull)
{
    *resnull = false;

    // Handle different JSONB value types
    switch (item->type) {
        case jbvNull:
            *resnull = true;
            return NULL;

        case jbvString:
            // Manual string copy with null termination
            char *str = palloc(item->val.string.len + 1);
            memcpy(str, item->val.string.val, item->val.string.len);
            str[item->val.string.len] = '\0';
            return str;

        case jbvNumeric:
            return DatumGetCString(DirectFunctionCall1(numeric_out,
                                  NumericGetDatum(item->val.numeric)));

        case jbvBool:
            return DatumGetCString(DirectFunctionCall1(boolout,
                                  BoolGetDatum(item->val.boolean)));

        case jbvDatetime:
            // Handle different datetime types
            switch (item->val.datetime.typid) {
                case DATEOID:
                    return DatumGetCString(DirectFunctionCall1(date_out,
                                          item->val.datetime.value));
                case TIMEOID:
                    return DatumGetCString(DirectFunctionCall1(time_out,
                                          item->val.datetime.value));
                // ... other datetime types similar
                default:
                    elog(ERROR, "unexpected jsonb datetime type oid %u",
                         item->val.datetime.typid);
            }

        case jbvArray:
        case jbvObject:
        case jbvBinary:
            // Convert complex types back to JSONB string
            return DatumGetCString(DirectFunctionCall1(jsonb_out,
                                  JsonbPGetDatum(JsonbValueToJsonb(item))));

        default:
            elog(ERROR, "unexpected jsonb value type %d", item->type);
    }

    return NULL;
}
```