# appendValue

## Location
[src/backend/utils/adt/jsonb_util.c:772-784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L772-L784)

## Overview
Completes a key-value pair by appending the value part to a JSONB object, incrementing the pair count in the process.

## Definition
```c
static void appendValue(JsonbParseState *pstate, JsonbValue *scalarVal)
```

## Detailed Description
This function serves as the second half of the key-value pair insertion process for JSONB objects. After appendKey has prepared the key portion of a pair, appendValue completes the operation by storing the associated value and incrementing the object's pair count. 

The function operates on the assumption that the key has already been set by a prior call to appendKey, and it directly assigns the value to the current pair position before advancing the nPairs counter. This simple but critical operation maintains the integrity of the key-value relationship within the JSONB object structure.

The function is designed to work with scalar values only, as validated by the calling code, and assumes proper memory allocation has already been handled by appendKey.

## Parameters / Member Variables
- `pstate`: Pointer to the current parse state containing the object being constructed
- `scalarVal`: Pointer to the JsonbValue containing the value to be added to the current key-value pair

## Dependencies
- Functions called/Symbols referenced:
  - jbvObject (JSONB object type constant)
  - [JsonbParseState](../J/JsonbParseState.md) (parse state structure)
- Called from (representative examples):
  - [pushJsonbValueScalar](../p/pushJsonbValueScalar.md) (when processing WJB_VALUE tokens and nested objects)

## Notes and Other Information
- This is a static function internal to jsonb_util.c, not exposed in the public API
- Must be called after appendKey to complete a key-value pair
- Uses assertions to validate that the container is indeed a JSONB object
- Increments nPairs counter as part of the value assignment operation
- Works with the assumption that memory allocation has been handled by appendKey
- Simple and efficient implementation focusing on completing the pair insertion
- Part of the sequential JSONB construction process alongside appendKey

## Simplified Source

```c
static void
appendValue(JsonbParseState *pstate, JsonbValue *scalarVal)
{
    JsonbValue *object = &pstate->contVal;

    Assert(object->type == jbvObject);

    // Complete the key-value pair and increment count
    object->val.object.pairs[object->val.object.nPairs++].value = *scalarVal;
}
```