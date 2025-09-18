# getKeyJsonValueFromContainer

## Location
src/backend/utils/adt/jsonb_util.c: 395 - 464

## Overview
Performs efficient binary search lookup of values by string key within a JSONB object container and returns the associated value.

## Definition


## Detailed Description
This function implements key-based value retrieval from JSONB objects using binary search for optimal performance. It takes advantage of the fact that JSONB objects store their key-value pairs in sorted order by key. The function searches through the object's keys to find an exact match, then retrieves and returns the corresponding value.

The search algorithm uses binary search on the key portion of key-value pairs, comparing string keys using lengthCompareJsonbString for accurate Unicode-aware comparison. Once a matching key is found, it calculates the index of the corresponding value (which is stored at key_index + count in the JEntry array) and uses fillJsonbValue to extract the value.

The function supports flexible memory management by allowing the caller to either provide a pre-allocated JsonbValue structure or request a new one.

## Parameters / Member Variables
- : The JSONB object container to search within (must be an object, not an array)
- : Pointer to the string key to search for
- : Length of the key string in bytes
- : Optional pre-allocated JsonbValue to fill, or NULL to allocate a new one

## Dependencies
- Functions called/Symbols referenced:
  - JsonContainerIsObject
  - JsonContainerSize
  - [getJsonbOffset](getJsonbOffset.md)
  - [getJsonbLength](getJsonbLength.md)
  - [lengthCompareJsonbString](../l/lengthCompareJsonbString.md)
  - [fillJsonbValue](../f/fillJsonbValue.md)
- Called from (representative examples):
  - [findJsonbValueFromContainer](../f/findJsonbValueFromContainer.md)
  - [JsonbDeepContains](../J/JsonbDeepContains.md)
  - [jsonb_object_field](../j/jsonb_object_field.md)
  - [jsonb_object_field_text](../j/jsonb_object_field_text.md)
  - [jsonb_get_element](../j/jsonb_get_element.md)
  - [JsObjectGetField](../J/JsObjectGetField.md)

## Notes and Other Information
- Uses binary search algorithm for O(log n) lookup performance on sorted JSONB object keys
- Asserts that the container is an object type (will fail on arrays)
- Optimized for empty objects: returns NULL immediately without memory allocation
- Memory management: if res is NULL, allocates and returns new JsonbValue; otherwise fills provided structure
- Key comparison is Unicode-aware through lengthCompareJsonbString
- Object storage layout: keys stored first, followed by corresponding values at indices [count, 2*count)