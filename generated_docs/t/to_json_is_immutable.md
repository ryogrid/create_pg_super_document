# to_json_is_immutable

## Location
src/backend/utils/adt/json.c: 691 - 729

## Overview
Determines whether a given PostgreSQL data type produces immutable JSON output when converted from a JSON context.

## Definition


## Detailed Description
The  function analyzes a PostgreSQL data type (identified by its OID) to determine whether converting values of that type to JSON will produce consistent, immutable results. This function is crucial for query optimization, as immutable expressions can be pre-evaluated and cached.

The function uses  to classify the input type into one of several JSON type categories, then applies specific rules to determine immutability. Date/time types are considered mutable because they depend on timezone settings. Array and composite types are currently marked as mutable, though the comments indicate future plans to recursively analyze their elements/fields. For numeric, cast, and other types, the function checks the volatility of the type's output function.

## Parameters / Member Variables
- : The PostgreSQL type OID to analyze for JSON immutability

## Dependencies
- Functions called/Symbols referenced:
  -  (categorizes the type for JSON processing)
  -  (checks the volatility level of output functions)
  -  enum values (JSONTYPE_BOOL, JSONTYPE_JSON, etc.)
  -  constant
- Called from:
  -  in query optimization
  - Referenced in 

## Notes and Other Information
- Returns  for: boolean, JSON, JSONB, and NULL types
- Returns  for: date, timestamp, timestamptz types (timezone-dependent)
- Returns  for: arrays and composites (TODO: implement recursive checking)
- For numeric, cast, and other types: returns  only if the output function is immutable
- This function is used by the PostgreSQL query optimizer to determine if JSON conversion expressions can be pre-computed
- Future enhancements may include recursive analysis of array elements and composite type fields
- Located in 