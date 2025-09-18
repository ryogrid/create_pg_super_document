# appendKey

## Location
[src/backend/utils/adt/jsonb_util.c:743-771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L743-L771)

## Overview
Appends a key to a JSONB object during construction, managing memory allocation and enforcing limits on the number of key-value pairs.

## Definition
```c
static void appendKey(JsonbParseState *pstate, JsonbValue *string)
```

## Detailed Description
This function handles the addition of keys to JSONB objects during the parsing and construction process. It performs several important operations:

1. **Validation**: Ensures that the current container is indeed a JSONB object and that the provided key is a string value
2. **Limit Enforcement**: Checks against JSONB_MAX_PAIRS to prevent excessive memory usage and maintain reasonable object sizes
3. **Memory Management**: Dynamically grows the pairs array when needed, doubling the size each time to amortize allocation costs
4. **Key Storage**: Stores the key string and assigns an order value for maintaining insertion order

The function works in conjunction with appendValue to complete the key-value pair addition process. The order field is crucial for maintaining the original insertion sequence of object properties.

## Parameters / Member Variables
- `pstate`: Pointer to the current parse state containing the object being constructed
- `string`: Pointer to the JsonbValue containing the key string to be added

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md) (PostgreSQL memory reallocation)
  - ereport/errcode/errmsg (PostgreSQL error reporting)
  - JSONB_MAX_PAIRS (maximum allowed pairs constant)
  - [JsonbPair](../J/JsonbPair.md) (structure type for key-value pairs)
- Called from (representative examples):
  - [pushJsonbValueScalar](../p/pushJsonbValueScalar.md) (when processing WJB_KEY tokens)

## Notes and Other Information
- This is a static function internal to jsonb_util.c, not exposed in the public API
- Uses assertions to validate input conditions and maintain invariants
- Implements exponential growth strategy for memory allocation (doubling size)
- The order field preserves insertion sequence, important for deterministic output
- Error reporting follows PostgreSQL conventions with specific error codes
- Works as part of a two-step process: appendKey followed by appendValue
- Memory management uses PostgreSQL's memory context system through repalloc