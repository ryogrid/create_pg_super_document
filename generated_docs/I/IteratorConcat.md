# IteratorConcat

## Location
[src/backend/utils/adt/jsonfuncs.c:5052-5179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5052-L5179)

## Overview
IteratorConcat is a static function that merges two JSON objects or arrays into one, handling all combinations of object-object, array-array, object-array, and array-object concatenations.

## Definition

```c
static JsonbValue *
IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
			   JsonbParseState **state)
```
## Detailed Description
This function iterates over two JsonbIterator instances and merges their contents into a single JsonbValue. The logic is adapted from a similar hstore function with special handling for JSON objects and arrays. The function supports four different concatenation scenarios:

1. **Object + Object**: Merges keys from both objects, with the second object's keys overriding duplicates from the first
2. **Array + Array**: Concatenates all elements from both arrays in sequence
3. **Object + Array**: Creates a new array containing the object as the first element followed by all array elements
4. **Array + Object**: Creates a new array containing all array elements followed by the object as the last element

The function uses JsonbIteratorNext to traverse the input iterators and pushJsonbValue to build the result structure incrementally.

## Parameters / Member Variables
- `**it1`: Pointer to the first JsonbIterator to be concatenated
- `**it2`: Pointer to the second JsonbIterator to be concatenated
- `**state`: Pointer to JsonbParseState used for building the result structure
## Dependencies
- Functions called/Symbols referenced:
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - WJB_BEGIN_OBJECT, WJB_BEGIN_ARRAY, WJB_END_OBJECT, WJB_END_ARRAY, WJB_ELEM, WJB_DONE (JsonbIteratorToken constants)
- Called from (representative examples):
  - [jsonb_concat](../j/jsonb_concat.md)

## Notes and Other Information
- This is a static function internal to jsonfuncs.c
- The function handles raw scalars by treating them as single-element arrays via JsonbIteratorNext
- For object concatenation, duplicate keys from the second object automatically override values from the first object
- The function preserves the order of elements when concatenating arrays
- Memory management is handled through the JsonbParseState mechanism