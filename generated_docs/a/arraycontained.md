# arraycontained

## Location
[src/backend/utils/adt/arrayfuncs.c:4548-4584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4548-L4584)

## Overview
This function determines whether one array is contained within (is a subset of) another array by checking if all elements of the first array exist in the second array.

## Definition

```c
Datum
arraycontained(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the '<@' operator for arrays in PostgreSQL. It takes two arrays as input and returns a boolean indicating whether the first array is contained within the second array (i.e., all elements of the first array exist in the second array). The function delegates the actual comparison logic to  with the arrays in their natural order for containment checking. Like its counterpart , it handles memory management by freeing any toasted input arrays.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (argument 0): The array being checked for containment (left operand of '<@')
  -  (argument 1): The array that potentially contains the elements (right operand of '<@')

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts array arguments from function call
  - : Gets collation information for string comparisons
  - : Core comparison function that performs the containment check
  - : Frees memory for toasted arrays
  - : Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL expressions using the '<@' operator
  - Array containment operations in queries

## Notes and Other Information
- This function implements the '<@' contained-by operator for arrays
- The arguments are passed to  in their natural order (array1, array2) unlike 
- Complementary to the  function -  is equivalent to 
- The function includes memory management to handle toasted (compressed/external) arrays
- Uses collation-aware comparison for elements that support collation (like text)
- Returns true if all elements of array1 exist in array2, false otherwise

## Simplified Source

```c
Datum
arraycontained(PG_FUNCTION_ARGS)
{
    AnyArrayType *array1 = PG_GETARG_ANY_ARRAY_P(0);
    AnyArrayType *array2 = PG_GETARG_ANY_ARRAY_P(1);
    Oid collation = PG_GET_COLLATION();

    // Check if array1 is contained in array2 (matchall = true)
    bool result = array_contain_compare(array1, array2, collation, true,
                                       &fcinfo->flinfo->fn_extra);

    // Clean up memory for toasted arrays
    AARR_FREE_IF_COPY(array1, 0);
    AARR_FREE_IF_COPY(array2, 1);

    PG_RETURN_BOOL(result);
}
```