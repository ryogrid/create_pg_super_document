# to_json_is_immutable

## Location
[src/backend/utils/adt/json.c:691-729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L691-L729)

## Overview
Determines whether a given PostgreSQL data type produces immutable JSON output when converted from a JSON context.

## Definition

```c
bool
to_json_is_immutable(Oid typoid)
```
## Detailed Description
The  function analyzes a PostgreSQL data type (identified by its OID) to determine whether converting values of that type to JSON will produce consistent, immutable results. This function is crucial for query optimization, as immutable expressions can be pre-evaluated and cached.

The function uses  to classify the input type into one of several JSON type categories, then applies specific rules to determine immutability. Date/time types are considered mutable because they depend on timezone settings. Array and composite types are currently marked as mutable, though the comments indicate future plans to recursively analyze their elements/fields. For numeric, cast, and other types, the function checks the volatility of the type's output function.

## Parameters / Member Variables
- `typoid`: The PostgreSQL type OID to analyze for JSON immutability
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

## Simplified Source

```c
bool
to_json_is_immutable(Oid typoid)
{
    JsonTypeCategory tcategory;
    Oid outfuncoid;

    // Categorize the type for JSON processing
    json_categorize_type(typoid, false, &tcategory, &outfuncoid);

    switch (tcategory)
    {
        case JSONTYPE_BOOL:
        case JSONTYPE_JSON:
        case JSONTYPE_JSONB:
        case JSONTYPE_NULL:
            return true;

        case JSONTYPE_DATE:
        case JSONTYPE_TIMESTAMP:
        case JSONTYPE_TIMESTAMPTZ:
            // Date/time types depend on timezone settings
            return false;

        case JSONTYPE_ARRAY:
            // TODO: recursively check array elements
            return false;

        case JSONTYPE_COMPOSITE:
            // TODO: recursively check composite fields
            return false;

        case JSONTYPE_NUMERIC:
        case JSONTYPE_CAST:
        case JSONTYPE_OTHER:
            // Check if output function is immutable
            return func_volatile(outfuncoid) == PROVOLATILE_IMMUTABLE;
    }

    return false;
}
``` 