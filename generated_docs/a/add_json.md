# add_json

## Location
[src/backend/utils/adt/json.c:593-620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L593-L620)

## Overview
A convenience wrapper function that converts a PostgreSQL Datum value to JSON and appends it to a StringInfo buffer, handling type categorization and null values automatically.

## Definition
```c
static void add_json(Datum val, bool is_null, StringInfo result,
                     Oid val_type, bool key_scalar)
```

## Detailed Description
add_json serves as a simplified interface for JSON conversion that combines type categorization and value conversion in a single call. It validates that the input type OID is valid, handles NULL values by setting appropriate category and output function parameters, and calls json_categorize_type to determine the conversion approach for non-NULL values. The function then delegates to datum_to_json_internal for the actual conversion work. While convenient for single-use conversions, the function's comments note that it's less efficient for repeated conversions of the same type due to repeated categorization overhead.

## Parameters / Member Variables
- `val`: PostgreSQL Datum containing the value to convert to JSON
- `is_null`: Boolean indicating whether the value is NULL
- `result`: StringInfo buffer where the JSON output is appended
- `val_type`: OID of the PostgreSQL type of the value
- `key_scalar`: Boolean indicating whether the value is used as a scalar key

## Dependencies
- Functions called/Symbols referenced:
  - [json_categorize_type](../j/json_categorize_type.md) (determine JSON conversion category and output function)
  - [datum_to_json_internal](../d/datum_to_json_internal.md) (perform the actual JSON conversion)
  - JSONTYPE_NULL (constant for NULL value category)
  - ereport, errcode, errmsg (error reporting for invalid type OID)
- Called from (representative examples):
  - [json_build_object_worker](../j/json_build_object_worker.md)
  - [json_build_array_worker](../j/json_build_array_worker.md)

## Notes and Other Information
The function includes input validation to ensure val_type is not InvalidOid, reporting an error if an invalid type is encountered. For NULL values, it bypasses type categorization and directly sets JSONTYPE_NULL category with InvalidOid output function. The function is designed as a convenience wrapper but carries a performance warning - applications converting many values of the same type should perform json_categorize_type once and reuse the results rather than calling this function repeatedly.