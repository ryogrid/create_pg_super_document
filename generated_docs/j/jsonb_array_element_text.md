# jsonb_array_element_text

## Location
[src/backend/utils/adt/jsonfuncs.c:978-1006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L978-L1006)

## Overview
Extracts the specified array element from a JSONB array and returns it as text, with proper conversion of JSONB values to their text representation.

## Definition
```c
Datum jsonb_array_element_text(PG_FUNCTION_ARGS)
```

## Detailed Description
This function extracts an element at a specified index from a JSONB array and converts it to text format. Unlike the JSON variant, this function works with the binary JSONB format which allows for more efficient processing. The function supports negative indexing (where -1 refers to the last element) and handles type conversion from JSONB values to text representation.

The function first validates that the input is indeed a JSONB array, then handles negative indices by converting them to positive indices, retrieves the element using container access functions, and finally converts the JSONB value to text using the JsonbValueAsText function.

## Parameters / Member Variables
- `jb` (Jsonb*): The input JSONB array as a PostgreSQL JSONB value
- `element` (int32): Zero-based index of the array element to extract (supports negative indexing)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P` - PostgreSQL macro to get JSONB argument
  - `PG_GETARG_INT32` - PostgreSQL macro to get integer argument
  - `JB_ROOT_IS_ARRAY` - Macro to check if JSONB root is an array
  - `JB_ROOT_COUNT` - Macro to get count of elements in JSONB root container
  - [getIthJsonbValueFromContainer](../g/getIthJsonbValueFromContainer.md) - Function to retrieve the i-th value from a JSONB container
  - [JsonbValueAsText](../J/JsonbValueAsText.md) - Function to convert JSONB value to text representation
  - `PG_RETURN_TEXT_P` - PostgreSQL macro to return text result
  - `PG_RETURN_NULL` - PostgreSQL macro to return NULL
- Called from (representative examples):
  - No direct references found (used via SQL function calls)

## Notes and Other Information
- Supports negative indexing where -1 refers to the last element, -2 to the second-to-last, etc.
- Returns NULL if the input is not a JSONB array, if the index is out of bounds, or if the retrieved value is JSON null
- More efficient than the JSON variant since it works with the binary JSONB format
- Uses `JsonbValueAsText` for proper conversion of different JSONB value types to text
- Part of PostgreSQL's JSONB support providing text extraction capabilities with better performance than JSON equivalents
- The function is registered as a PostgreSQL built-in function and accessible via SQL

## Simplified Source

```c
Datum
jsonb_array_element_text(PG_FUNCTION_ARGS)
{
    Jsonb *jb = PG_GETARG_JSONB_P(0);
    int element = PG_GETARG_INT32(1);
    JsonbValue *v;

    // Verify input is a JSONB array
    if (!JB_ROOT_IS_ARRAY(jb))
        PG_RETURN_NULL();

    // Convert negative indices to positive (e.g., -1 becomes last element)
    if (element < 0) {
        uint32 nelements = JB_ROOT_COUNT(jb);
        if (-element > nelements)
            PG_RETURN_NULL();
        element += nelements;
    }

    // Extract the element at the specified index
    v = getIthJsonbValueFromContainer(&jb->root, element);

    // Convert JSONB value to text if valid and non-null
    if (v != NULL && v->type != jbvNull)
        PG_RETURN_TEXT_P(JsonbValueAsText(v));

    PG_RETURN_NULL();
}
```