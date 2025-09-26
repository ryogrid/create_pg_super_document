# datum_to_jsonb_internal

## Location
[src/backend/utils/adt/jsonb.c:638-861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L638-L861)

## Overview
Converts a PostgreSQL Datum value into a JsonbValue and adds it to the result JsonbInState, handling all data types that can be represented in JSONB format.

## Definition
```c
static void datum_to_jsonb_internal(Datum val, bool is_null, JsonbInState *result,
                                   JsonTypeCategory tcategory, Oid outfuncoid,
                                   bool key_scalar)
```

## Detailed Description
The `datum_to_jsonb_internal` function is a core component of PostgreSQL's JSONB conversion system. It takes a Datum value along with its type information and converts it into the appropriate JsonbValue representation, then integrates it into the JsonbInState parsing structure. The function handles a wide variety of PostgreSQL data types including scalars, arrays, composite types, and existing JSON/JSONB values.

The function performs different conversions based on the JsonTypeCategory:
- **JSONTYPE_ARRAY**: Delegates to array_to_jsonb_internal for array processing
- **JSONTYPE_COMPOSITE**: Delegates to composite_to_jsonb for record/composite types
- **JSONTYPE_BOOL**: Converts boolean values, with special handling for key contexts
- **JSONTYPE_NUMERIC**: Handles numeric values with validation for JSON number format
- **JSONTYPE_DATE/TIMESTAMP/TIMESTAMPTZ**: Converts temporal types to ISO string format
- **JSONTYPE_JSON/JSONB**: Parses existing JSON text or processes JSONB structures
- **Default**: Converts other types to strings using their output functions

Special considerations include key_scalar mode where certain complex types are rejected for use as object keys, and proper handling of null values and scalar JSONB roots.

## Parameters / Member Variables
- `val`: The Datum value to be converted to JSONB
- `is_null`: Boolean indicating if the value is SQL NULL
- `result`: JsonbInState structure to accumulate the conversion result
- `tcategory`: JsonTypeCategory indicating the data type classification
- `outfuncoid`: OID of the output function for the data type
- `key_scalar`: Boolean flag indicating if this value will be used as an object key

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [array_to_jsonb_internal](../a/array_to_jsonb_internal.md)
  - [composite_to_jsonb](../c/composite_to_jsonb.md)
  - OidFunctionCall1
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - DirectFunctionCall3
  - [numeric_in](../n/numeric_in.md)
  - [DatumGetNumeric](../D/DatumGetNumeric.md)
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md)
  - DatumGetTextPP
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - pg_parse_json_or_ereport
  - [freeJsonLexContext](../f/freeJsonLexContext.md)
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [checkStringLen](../c/checkStringLen.md)
- Called from (representative examples):
  - [array_dim_to_jsonb](../a/array_dim_to_jsonb.md)
  - [composite_to_jsonb](../c/composite_to_jsonb.md)
  - [add_jsonb](../a/add_jsonb.md)
  - [datum_to_jsonb](datum_to_jsonb.md)
  - [jsonb_agg_transfn_worker](../j/jsonb_agg_transfn_worker.md)
  - [jsonb_object_agg_transfn_worker](../j/jsonb_object_agg_transfn_worker.md)

## Notes and Other Information
- This is a static function used internally within jsonb.c for type conversion
- Assumes result->escontext is NULL and errors will be thrown rather than handled gracefully
- Key scalar mode enforces restrictions on complex types (arrays, composites, JSON) to prevent invalid object keys
- For numeric types, performs validation to ensure the string representation is valid JSON (checking for 'N' or 'n' characters)
- Handles both scalar and non-scalar JSONB inputs with different processing paths
- Uses recursive parsing for JSON text input and iterative processing for existing JSONB structures
- The function is designed to be part of a larger parsing state machine managed by JsonbInState