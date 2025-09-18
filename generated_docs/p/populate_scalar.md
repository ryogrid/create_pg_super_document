# populate_scalar

## Location
src/backend/utils/adt/jsonfuncs.c: 3123 - 3214

## Overview
Populates a non-null scalar value from JSON/JsonB input, handling type conversion and string formatting for various PostgreSQL data types.

## Definition
```c
static Datum populate_scalar(ScalarIOData *io, Oid typid, int32 typmod, JsValue *jsv,
                            bool *isnull, Node *escontext, bool omit_quotes)
```

## Detailed Description
populate_scalar converts JSON/JsonB values into PostgreSQL scalar data types through a comprehensive type conversion process. The function handles two primary input formats:

**Plain JSON processing**: 
- Manages both null-terminated and length-specified JSON strings
- Applies JSON literal escaping for JSON/JSONB target types when source is a JSON_TOKEN_STRING

**JsonB binary processing**:
- Handles different JsonB value types (string, boolean, numeric, binary)
- Provides optimized direct conversion for JsonB-to-JsonB operations
- Converts JsonB values to appropriate string representations for other target types
- Supports quote omission for string values when requested

The function uses InputFunctionCallSafe() for the final type conversion, ensuring safe error handling through the escontext mechanism.

## Parameters / Member Variables
- `io`: ScalarIOData structure containing type input function information
- `typid`: Target PostgreSQL type OID for the conversion
- `typmod`: Type modifier for the target type
- `jsv`: Input JSON/JsonB value structure
- `isnull`: Pointer to NULL indicator flag (output)
- `escontext`: Error context for soft error handling
- `omit_quotes`: Whether to strip quotes from string values during conversion

## Dependencies
- Functions called/Symbols referenced:
  - unconstify
  - [escape_json](../e/escape_json.md)
  - [pnstrdup](pnstrdup.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - [JsonbPGetDatum](../J/JsonbPGetDatum.md)
  - [JsonbToCString](../J/JsonbToCString.md)
  - [pstrdup](pstrdup.md)
  - DirectFunctionCall1
  - [numeric_out](../n/numeric_out.md)
  - [DatumGetCString](../D/DatumGetCString.md)
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md)
- Called from (representative examples):
  - [populate_record_field](populate_record_field.md)
  - JsObjectFree

## Notes and Other Information
- Returns NULL datum and sets *isnull = true when type input function calls fail
- Optimizes JsonB-to-JsonB conversions by bypassing string conversion entirely
- Handles memory management carefully, freeing temporary string buffers when needed
- Supports both positive and negative length specifications for JSON strings
- Preserves JSON literal formatting for JSON/JSONB target types by applying proper escaping
- Converts different JsonB scalar types (string, boolean, numeric, binary) to their string representations
- Uses safe input functions to allow graceful error handling rather than throwing exceptions
- The omit_quotes parameter allows flexible handling of string quotation marks depending on context
- Handles binary JsonB containers by converting them to C strings for input processing