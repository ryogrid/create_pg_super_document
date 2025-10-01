# getKeyJsonValueFromContainer

## Location
[src/backend/utils/adt/jsonb_util.c:395-464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L395-L464)

## Overview
Performs efficient binary search lookup of values by string key within a JSONB object container and returns the associated value.

## Definition

```c
JsonbValue *
getKeyJsonValueFromContainer(JsonbContainer *container,
							 const char *keyVal, int keyLen, JsonbValue *res)
```
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

## Simplified Source

```c
JsonbValue *
getKeyJsonValueFromContainer(JsonbContainer *container,
                            const char *keyVal, int keyLen, JsonbValue *res) {
    JEntry *children = container->children;
    int count = JsonContainerSize(container);
    char *baseAddr;
    uint32 stopLow, stopHigh;

    Assert(JsonContainerIsObject(container));

    // Quick exit for empty objects
    if (count <= 0)
        return NULL;

    // Binary search through object keys
    baseAddr = (char *) (children + count * 2);
    stopLow = 0;
    stopHigh = count;

    while (stopLow < stopHigh) {
        uint32 stopMiddle = stopLow + (stopHigh - stopLow) / 2;

        // Get candidate key from object
        const char *candidateVal = baseAddr + getJsonbOffset(container, stopMiddle);
        int candidateLen = getJsonbLength(container, stopMiddle);

        // Compare with target key
        int difference = lengthCompareJsonbString(candidateVal, candidateLen,
                                                 keyVal, keyLen);

        if (difference == 0) {
            // Found matching key, get corresponding value
            int index = stopMiddle + count;

            if (!res)
                res = palloc(sizeof(JsonbValue));

            fillJsonbValue(container, index, baseAddr,
                          getJsonbOffset(container, index), res);
            return res;
        } else if (difference < 0) {
            stopLow = stopMiddle + 1;
        } else {
            stopHigh = stopMiddle;
        }
    }

    return NULL;  // Key not found
}
```