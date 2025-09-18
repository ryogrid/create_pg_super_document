# transform_jsonb_string_values

## Location
src/backend/utils/adt/jsonfuncs.c: 5782 - 5828

## Overview
Iterates over a JSONB structure and applies a transformation function to every string value, returning a new JSONB object with the transformed values.

## Definition
```c
Jsonb *transform_jsonb_string_values(Jsonb *jsonb, void *action_state,
                                    JsonTransformStringValuesAction transform_action)
```

## Detailed Description
This function provides a mechanism for transforming string values within a JSONB structure while preserving the overall structure and all non-string values. It iterates through the entire JSONB using the JSONB iterator infrastructure, identifies string values and elements, and applies a user-defined transformation function to each string. The transformation function receives the original string data and length, and returns a new text datum representing the transformed string. The function constructs a new JSONB object with the transformed strings while maintaining the original structure of objects, arrays, and other data types.

## Parameters / Member Variables
- `jsonb`: The source JSONB structure to transform
- `action_state`: User-defined state object passed through to the transformation function
- `transform_action`: Callback function of type JsonTransformStringValuesAction that transforms individual string values

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pg_detoast_datum_packed](../p/pg_detoast_datum_packed.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
- Called from (representative examples):
  - [ts_headline_jsonb_byid_opt](ts_headline_jsonb_byid_opt.md)
  - pg_parse_json_or_ereport

## Notes and Other Information
The function handles memory management by detoasting the transformed text values to ensure proper memory layout. It preserves the scalar nature of the original JSONB if it was a scalar value. The transformation is applied only to string values (jbvString type) within WJB_VALUE and WJB_ELEM contexts, leaving object keys, numeric values, boolean values, and structural elements unchanged. The function constructs the result incrementally using pushJsonbValue and converts it back to a complete JSONB structure using JsonbValueToJsonb.