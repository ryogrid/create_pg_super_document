# JsonItemFromDatum

## Location
src/backend/utils/adt/jsonpath_exec.c: 3049 - 3130

## Overview
Initializes a JsonbValue structure from a PostgreSQL Datum value of a specified type, converting various PostgreSQL data types into their JSON equivalents.

## Definition


## Detailed Description
This function serves as a comprehensive type converter that transforms PostgreSQL Datum values into JsonbValue structures suitable for JSON operations. It handles a wide variety of PostgreSQL data types including booleans, numeric types (integers, floats, numeric), text types, datetime types, and JSON types (both JSON and JSONB). For numeric types, it converts them to PostgreSQL's numeric type for consistent JSON representation. For datetime types, it preserves the original type information. For JSON/JSONB types, it handles both scalar and complex values appropriately, including recursive conversion for JSON text.

## Parameters / Member Variables
- : Datum value to be converted (the actual PostgreSQL data value)
- : Oid specifying the PostgreSQL type of the input value
- : int32 type modifier providing additional type information (e.g., precision)
- : JsonbValue pointer where the converted result will be stored (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetBool (extracts boolean from Datum)
  - JsonbValueInitNumericDatum (initializes numeric JsonbValue)
  - DirectFunctionCall1 (calls PostgreSQL functions)
  - int2_numeric, int4_numeric, int8_numeric, float4_numeric, float8_numeric (numeric conversion functions)
  - VARDATA_ANY, VARSIZE_ANY_EXHDR (variable-length data macros)
  - DatumGetJsonbP (extracts JSONB from Datum)
  - JsonContainerIsScalar (checks if JSONB container is scalar)
  - JsonbExtractScalar (extracts scalar value from JSONB)
  - JsonbInitBinary (initializes binary JSONB value)
  - DatumGetTextP, text_to_cstring (text conversion functions)
  - jsonb_in (JSON text to JSONB conversion)
  - JsonbPGetDatum (converts JSONB to Datum)
  - format_type_be (formats type name for error messages)
- Data types used:
  - Various PostgreSQL type OIDs: BOOLOID, NUMERICOID, INT2OID, INT4OID, INT8OID, FLOAT4OID, FLOAT8OID, TEXTOID, VARCHAROID, DATEOID, TIMEOID, TIMETZOID, TIMESTAMPOID, TIMESTAMPTZOID, JSONBOID, JSONOID
  - JSON value types: jbvBool, jbvString, jbvDatetime
- Called from (representative examples):
  - RETURN_ERROR macro in jsonpath_exec.c:315
  - GetJsonPathVar in jsonpath_exec.c:3027
  - JsonItemFromDatum (recursive call) in jsonpath_exec.c:3118

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- Handles recursive conversion for JSON text type by converting to JSONB first
- Uses PostgreSQL's numeric type as the canonical representation for all numeric values in JSON
- Preserves timezone and type modifier information for datetime types
- Throws an error for unsupported data types with a descriptive message
- Part of PostgreSQL's SQL/JSON type conversion system
- The function modifies the output parameter 'res' rather than returning a value