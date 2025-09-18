# uniqueifyJsonbObject

## Location
src/backend/utils/adt/jsonb_util.c: 1949 - 1998

## Overview
Sorts and removes duplicate key-value pairs from a JsonbValue object, with options to enforce unique keys and skip null values during the process.

## Definition


## Detailed Description
This function performs sorting and deduplication on JSON object key-value pairs within a JsonbValue structure. It first sorts all pairs using lengthCompareJsonbPair as the comparator, which performs length-wise string comparison on the keys. After sorting, the function can optionally remove duplicate keys and null values based on the provided flags.

When unique_keys is true and duplicates are detected, the function raises an error. When skip_nulls is true, the function removes all key-value pairs where the value is null (jbvNull). The deduplication process preserves the first occurrence of each key when duplicates exist, maintaining the original order semantics established by the sorting phase.

The function modifies the object in-place, potentially reducing the nPairs count and shifting the pairs array to remove unwanted elements.

## Parameters / Member Variables
- : Pointer to a JsonbValue object that must be of type jbvObject containing the pairs to be processed
- : Boolean flag indicating whether duplicate keys should cause an error to be raised
- : Boolean flag indicating whether key-value pairs with null values should be removed from the object

## Dependencies
- Functions called/Symbols referenced:
  - [lengthCompareJsonbPair](../l/lengthCompareJsonbPair.md)
  - qsort_arg
  - [lengthCompareJsonbStringValue](../l/lengthCompareJsonbStringValue.md)
  - ereport (error reporting)
  - memcpy
  - [JsonbPair](../J/JsonbPair.md) (struct type)
  - jbvObject (enum value)
  - jbvNull (enum value)
- Called from (representative examples):
  - [pushJsonbValueScalar](../p/pushJsonbValueScalar.md)

## Notes and Other Information
- This is a static function within jsonb_util.c, not exposed to other modules
- The function assumes the input object is of type jbvObject and will Assert if this precondition is not met
- Only sorts when there are more than 1 pairs for efficiency
- Uses length-wise string comparison for key ordering, which is specific to JSONB's internal representation
- The deduplication algorithm preserves the first occurrence of duplicate keys, which aligns with JSON semantics where later keys should overwrite earlier ones with the same name
- Memory management is handled efficiently by shifting array elements in-place rather than allocating new memory
- The function can handle edge cases like objects with only null values when skip_nulls is enabled