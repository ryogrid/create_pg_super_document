# JsonbPair

## Location
src/include/utils/jsonb.h: 311 - 318

## Overview
JsonbPair represents a key/value pair within a JSON object during construction, serving as a temporary in-memory structure for building JSONB objects.

## Definition
```c
struct JsonbPair
{
    JsonbValue  key;        /* Must be a jbvString */
    JsonbValue  value;      /* May be of any type */
    uint32      order;      /* Pair's index in original sequence */
};
```

## Detailed Description
JsonbPair is a temporary structure used exclusively during JSONB object construction and is not part of the on-disk representation. It holds a key/value pair where the key must always be a string type (jbvString) and the value can be any valid JSON type. The structure includes an order field to track the original sequence of pairs, which is essential for handling duplicate key resolution using a "last observed wins" strategy.

This structure is primarily used in the conversion process from text JSON to binary JSONB format, allowing the system to maintain proper key ordering and handle deduplication before finalizing the on-disk format.

## Parameters / Member Variables
- `key`: JsonbValue that must be of type jbvString, representing the object property name
- `value`: JsonbValue of any valid JSON type (string, number, boolean, null, array, object), representing the property value
- `order`: 32-bit unsigned integer tracking the pair's position in the original input sequence, used for duplicate key resolution

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbValue](JsonbValue.md) (for both key and value members)
  - uint32 (standard type)
  - jbvString (enum value indicating string type)
- Called from (representative examples):
  - [pushJsonbValueScalar](../p/pushJsonbValueScalar.md)
  - [appendKey](../a/appendKey.md)
  - [convertJsonbObject](../c/convertJsonbObject.md)
  - [lengthCompareJsonbPair](../l/lengthCompareJsonbPair.md)
  - [uniqueifyJsonbObject](../u/uniqueifyJsonbObject.md)

## Notes and Other Information
JsonbPair is only used temporarily during JSONB construction and is not persisted to disk. The order field enables proper duplicate key handling by implementing "last observed wins" semantics. Keys are required to be strings as per JSON specification, while values can be any valid JSON type. The structure is typically used in arrays that are later processed to create the final on-disk JsonbContainer format.