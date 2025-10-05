# jsonb_array_element

## Location
[src/backend/utils/adt/jsonfuncs.c:935-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L935-L962)

## Overview
Extracts an element from a JSONB array by index (supporting both positive and negative indices) and returns the element value as a JSONB value, or NULL if the index is out of bounds or the input is not an array.

## Definition
```c
Datum jsonb_array_element(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the JSONB array element access operator (->). It takes a JSONB value and an integer index as input parameters, retrieves the element at the specified index from the JSONB array, and returns the corresponding element as a JSONB datum. The function supports both positive (zero-based) and negative indices, where negative indices count backwards from the end of the array. The function performs type checking to ensure the input JSONB is an array before attempting element extraction.

The function handles negative indices by converting them to positive indices using the array length. It uses specialized JSONB container access functions for efficient element retrieval.

## Parameters / Member Variables
- `jb`: The input JSONB value representing an array from which to extract an element
- `element`: The integer index of the array element to retrieve (supports negative indices)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Retrieves JSONB argument from function call
  - PG_GETARG_INT32: Retrieves 32-bit integer argument from function call
  - JB_ROOT_IS_ARRAY: Macro to check if JSONB root is an array type
  - JB_ROOT_COUNT: Macro to get the count of elements in JSONB root container
  - [getIthJsonbValueFromContainer](../g/getIthJsonbValueFromContainer.md): Retrieves the i-th element from JSONB array container
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md): Converts JsonbValue to Jsonb format
  - PG_RETURN_JSONB_P: Returns JSONB result from function
  - PG_RETURN_NULL: Returns NULL result from function
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- This function is the backend implementation for the JSONB -> operator with integer indices in SQL
- Returns NULL if the input is not a JSONB array or if the specified index is out of bounds
- Supports negative indexing where -1 refers to the last element, -2 to second-to-last, etc.
- Negative index handling: converts negative indices to positive by adding the array length
- More efficient than the JSON equivalent as it works directly with binary JSONB format
- Uses specialized JSONB container functions for direct element access without parsing
- Located in src/backend/utils/adt/jsonfuncs.c:935-962

## Simplified Source

```c
Datum jsonb_array_element(PG_FUNCTION_ARGS) {
    Jsonb *jb = PG_GETARG_JSONB_P(0);
    int element = PG_GETARG_INT32(1);

    // Check if input is an array
    if (!JB_ROOT_IS_ARRAY(jb))
        PG_RETURN_NULL();

    // Handle negative indices (convert to positive)
    if (element < 0) {
        uint32 nelements = JB_ROOT_COUNT(jb);
        if (-element > nelements)
            PG_RETURN_NULL();
        element += nelements;
    }

    // Extract the element from JSONB container
    JsonbValue *v = getIthJsonbValueFromContainer(&jb->root, element);
    if (v != NULL)
        PG_RETURN_JSONB_P(JsonbValueToJsonb(v));

    PG_RETURN_NULL();
}
```