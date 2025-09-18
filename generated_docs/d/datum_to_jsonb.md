# datum_to_jsonb

## Location
src/backend/utils/adt/jsonb.c: 1112 - 1124

## Overview
A function that converts a PostgreSQL Datum value to JSONB format, serving as a core conversion utility that bridges PostgreSQL's internal data representation with JSONB.

## Definition


## Detailed Description
The datum_to_jsonb function performs the actual conversion of PostgreSQL Datum values to JSONB format. It takes a preprocessed type category and output function OID (typically obtained from json_categorize_type) to efficiently convert the input value. The function initializes a JsonbInState structure, delegates the conversion work to datum_to_jsonb_internal, and then packages the result into a proper JSONB Datum for return. This function is optimized for scenarios where type categorization has already been performed, avoiding redundant type lookups.

## Parameters / Member Variables
- : The PostgreSQL Datum value to be converted to JSONB
- : Pre-categorized JSON type category for the input value
- : OID of the output function to use for conversion (from prior json_categorize_type call)

## Dependencies
- Functions called/Symbols referenced:
  - datum_to_jsonb_internal
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - [JsonbPGetDatum](../J/JsonbPGetDatum.md)
  - [JsonbInState](../J/JsonbInState.md)
  - JsonTypeCategory
- Called from (representative examples):
  - [to_jsonb](../t/to_jsonb.md)
  - [ExecEvalJsonConstructor](../E/ExecEvalJsonConstructor.md)
  - JsonTypeCategory

## Notes and Other Information
- This function assumes val is not NULL (passes false for is_null parameter)
- Optimized for performance by accepting pre-computed type categorization information
- The result JsonbInState is zero-initialized before use
- Uses JsonbValueToJsonb to convert the internal representation to final JSONB format
- The key_scalar parameter is set to false, indicating this is not a JSON object key
- Central to PostgreSQL's JSONB conversion infrastructure and used throughout the system