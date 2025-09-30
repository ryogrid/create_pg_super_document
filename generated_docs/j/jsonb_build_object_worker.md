# jsonb_build_object_worker

## Location
[src/backend/utils/adt/jsonb.c:1125-1176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1125-L1176)

## Overview
A worker function that constructs a JSONB object from alternating key-value pairs, with support for null handling and key uniqueness validation.

## Definition

```c
Datum
jsonb_build_object_worker(int nargs, const Datum *args, const bool *nulls, const Oid *types,
						  bool absent_on_null, bool unique_keys)
```
## Detailed Description
The jsonb_build_object_worker function is the core implementation for building JSONB objects from a sequence of alternating key-value pairs. It validates that the argument count is even (since keys and values must be paired), initializes a JsonbInState for building the object, and processes each key-value pair while applying the specified null handling and uniqueness policies. The function enforces that keys cannot be null, but provides flexible handling of null values based on the absent_on_null parameter. When unique_keys is enabled, duplicate keys are detected and handled appropriately.

## Parameters / Member Variables
- : Total number of arguments (must be even for key-value pairs)
- : Array of Datum values representing alternating keys and values
- : Array of boolean flags indicating which arguments are NULL
- : Array of PostgreSQL type OIDs for each argument
- : Boolean flag to skip key-value pairs when the value is NULL
- : Boolean flag to enforce key uniqueness in the resulting object

## Dependencies
- Functions called/Symbols referenced:
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [add_jsonb](../a/add_jsonb.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - [JsonbPGetDatum](../J/JsonbPGetDatum.md)
  - [JsonbInState](../J/JsonbInState.md)
  - WJB_BEGIN_OBJECT, WJB_END_OBJECT
- Called from (representative examples):
  - [jsonb_build_object](jsonb_build_object.md)
  - [ExecEvalJsonConstructor](../E/ExecEvalJsonConstructor.md)
  - PG_RETURN_JSONB_P

## Notes and Other Information
- Validates that the number of arguments is even (key-value pairs)
- Enforces that keys cannot be NULL - raises an error if a key is NULL
- Supports absent_on_null mode where NULL values are omitted from the result
- When unique_keys is enabled, processes all keys even for skipped entries to enable uniqueness checking
- Uses the add_jsonb helper function to convert and add each key-value pair
- The function is central to PostgreSQL's jsonb_build_object() SQL function and JSON constructor expressions
- Handles complex logic around null value processing and key uniqueness validation

## Simplified Source

```c
Datum
jsonb_build_object_worker(int nargs, const Datum *args, const bool *nulls, const Oid *types,
                          bool absent_on_null, bool unique_keys)
{
    JsonbInState result;

    // Validate even number of arguments (key-value pairs)
    if (nargs % 2 != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("argument list must have even number of elements")));
    }

    // Initialize JSONB construction state
    memset(&result, 0, sizeof(JsonbInState));

    // Start building object with configuration
    result.res = pushJsonbValue(&result.parseState, WJB_BEGIN_OBJECT, NULL);
    result.parseState->unique_keys = unique_keys;
    result.parseState->skip_nulls = absent_on_null;

    // Process key-value pairs
    for (int i = 0; i < nargs; i += 2) {
        // Keys cannot be null
        if (nulls[i]) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("argument %d: key must not be null", i + 1)));
        }

        // Skip null values if absent_on_null is enabled
        bool skip = absent_on_null && nulls[i + 1];
        if (skip && !unique_keys)
            continue;

        // Add key-value pair
        add_jsonb(args[i], false, &result, types[i], true);        // key
        add_jsonb(args[i + 1], nulls[i + 1], &result, types[i + 1], false); // value
    }

    // Complete object construction
    result.res = pushJsonbValue(&result.parseState, WJB_END_OBJECT, NULL);

    // Convert to final JSONB datum
    return JsonbPGetDatum(JsonbValueToJsonb(result.res));
}
```