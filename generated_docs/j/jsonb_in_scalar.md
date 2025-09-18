# jsonb_in_scalar

## Location
src/backend/utils/adt/jsonb.c: 379 - 472

## Overview
A static function that processes scalar JSON tokens during JSONB parsing, converting them to JsonbValue structures and integrating them into the parse state.

## Definition


## Detailed Description
This function serves as a callback handler for scalar values during JSON to JSONB conversion. It processes different types of JSON scalar tokens (strings, numbers, booleans, null) and converts them to appropriate JsonbValue structures. The function handles both standalone scalar values and scalars within arrays or objects. For standalone scalars, it wraps them in a special array structure with the rawScalar flag set to true. The function integrates with PostgreSQL's JSON parsing infrastructure and includes proper error handling through the error context mechanism.

## Parameters / Member Variables
- : Void pointer to the JsonbInState structure containing parsing state information
- : String representation of the scalar token to be processed
- : JsonTokenType enum indicating the type of JSON token being processed

## Dependencies
- Functions called/Symbols referenced:
  - [checkStringLen](../c/checkStringLen.md) (for string length validation)
  - [DirectInputFunctionCallSafe](../D/DirectInputFunctionCallSafe.md), numeric_in, DatumGetNumeric (for numeric conversion)
  - [pushJsonbValue](../p/pushJsonbValue.md) (for building JSONB structure)
  - elog (for error reporting)
  - JSON token constants: JSON_TOKEN_STRING, JSON_TOKEN_NUMBER, JSON_TOKEN_TRUE, JSON_TOKEN_FALSE, JSON_TOKEN_NULL
  - JSONB value constants: jbvString, jbvNumeric, jbvBool, jbvNull, jbvArray, jbvObject
  - JSONB builder constants: WJB_BEGIN_ARRAY, WJB_ELEM, WJB_END_ARRAY, WJB_VALUE
- Called from (representative examples):
  - [jsonb_from_cstring](jsonb_from_cstring.md) (as part of JSON parsing callbacks)
  - datum_to_jsonb_internal (for internal JSONB conversion)

## Notes and Other Information
- This is a static function used internally within the JSONB parsing mechanism
- Returns JsonParseErrorType to indicate success (JSON_SUCCESS) or failure (JSON_SEM_ACTION_FAILED)
- Special handling for standalone scalar values: they are wrapped in an array with rawScalar=true to maintain JSONB's requirement that top-level values be containers
- Uses safe input functions with error context support for robust error handling
- Numeric values are processed through PostgreSQL's numeric input function for proper type conversion
- String length validation is performed to ensure compatibility with JSONB constraints
- The function properly integrates scalar values into existing array or object contexts during parsing