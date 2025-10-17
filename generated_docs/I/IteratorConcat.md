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

## Simplified Source

```c
static JsonbValue *IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
                                  JsonbParseState **state) {
    JsonbValue v1, v2, *res = NULL;
    JsonbIteratorToken rk1, rk2;

    // Get the root tokens to determine types
    rk1 = JsonbIteratorNext(it1, &v1, false);
    rk2 = JsonbIteratorNext(it2, &v2, false);

    if (rk1 == WJB_BEGIN_OBJECT && rk2 == WJB_BEGIN_OBJECT) {
        // Object + Object: merge keys, second object overrides duplicates
        pushJsonbValue(state, rk1, NULL);

        // Copy all from first object except end marker
        JsonbIteratorToken r1;
        while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_OBJECT)
            pushJsonbValue(state, r1, &v1);

        // Copy all from second object including end marker
        JsonbIteratorToken r2;
        while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
            res = pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);
    }
    else if (rk1 == WJB_BEGIN_ARRAY && rk2 == WJB_BEGIN_ARRAY) {
        // Array + Array: concatenate all elements
        pushJsonbValue(state, rk1, NULL);

        // Copy elements from first array
        JsonbIteratorToken r1;
        while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_ARRAY)
            pushJsonbValue(state, r1, &v1);

        // Copy elements from second array
        JsonbIteratorToken r2;
        while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_END_ARRAY)
            pushJsonbValue(state, WJB_ELEM, &v2);

        res = pushJsonbValue(state, WJB_END_ARRAY, NULL);
    }
    else if (rk1 == WJB_BEGIN_OBJECT) {
        // Object + Array: wrap in array [object, ...array_elements]
        pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

        // Add object as first element
        pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
        JsonbIteratorToken r1;
        while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_DONE)
            pushJsonbValue(state, r1, r1 != WJB_END_OBJECT ? &v1 : NULL);

        // Add array elements
        JsonbIteratorToken r2;
        while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
            res = pushJsonbValue(state, r2, r2 != WJB_END_ARRAY ? &v2 : NULL);
    }
    else {
        // Array + Object: wrap in array [...array_elements, object]
        pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

        // Add array elements first
        JsonbIteratorToken r1;
        while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_ARRAY)
            pushJsonbValue(state, r1, &v1);

        // Add object as last element
        pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
        JsonbIteratorToken r2;
        while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
            pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);

        res = pushJsonbValue(state, WJB_END_ARRAY, NULL);
    }

    return res;
}
```