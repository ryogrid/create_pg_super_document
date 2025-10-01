# add_jsonb

## Location
[src/backend/utils/adt/jsonb.c:1016-1048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1016-L1048)

## Overview
A static helper function that appends JSON text for a given value to a JsonbInState result structure, serving as a thin wrapper around datum_to_jsonb_internal.

## Definition

```c
static void
add_jsonb(Datum val, bool is_null, JsonbInState *result,
		  Oid val_type, bool key_scalar)
```
## Detailed Description
The add_jsonb function is a utility function that converts a PostgreSQL Datum value into JSONB format and appends it to an existing JsonbInState structure. It acts as a convenient wrapper around the more complex datum_to_jsonb_internal function by handling the type categorization step automatically. The function first validates the input type, then categorizes the PostgreSQL type into its corresponding JSON type category, and finally delegates the actual conversion work to datum_to_jsonb_internal.

## Parameters / Member Variables
- : The PostgreSQL Datum value to be converted to JSONB
- : Boolean flag indicating whether the value is NULL
- : Pointer to JsonbInState structure where the converted JSONB data will be appended
- : The PostgreSQL OID representing the data type of the value
- : Boolean flag indicating whether this value represents a scalar key in a JSON object

## Dependencies
- Functions called/Symbols referenced:
  - [json_categorize_type](../j/json_categorize_type.md)
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md)
  - [JsonbInState](../J/JsonbInState.md)
  - JsonTypeCategory
  - JSONTYPE_NULL
- Called from (representative examples):
  - [jsonb_build_object_worker](../j/jsonb_build_object_worker.md)
  - [jsonb_build_array_worker](../j/jsonb_build_array_worker.md)

## Notes and Other Information
- This function includes input validation to ensure val_type is not InvalidOid
- For NULL values, the function sets tcategory to JSONTYPE_NULL and outfuncoid to InvalidOid
- The function is designed as a convenience wrapper; for scenarios where the same type will be processed multiple times, it's more efficient to call json_categorize_type once and use datum_to_jsonb_internal directly
- The function is static, meaning it's only accessible within the jsonb.c compilation unit

## Simplified Source

```c
static void
add_jsonb(Datum val, bool is_null, JsonbInState *result,
          Oid val_type, bool key_scalar)
{
    JsonTypeCategory tcategory;
    Oid outfuncoid;

    // Validate input type
    if (val_type == InvalidOid)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("could not determine input data type")));

    // Handle NULL values
    if (is_null)
    {
        tcategory = JSONTYPE_NULL;
        outfuncoid = InvalidOid;
    }
    else
    {
        // Categorize PostgreSQL type to JSON type
        json_categorize_type(val_type, true, &tcategory, &outfuncoid);
    }

    // Convert datum to JSONB and append to result
    datum_to_jsonb_internal(val, is_null, result, tcategory, outfuncoid, key_scalar);
}
```