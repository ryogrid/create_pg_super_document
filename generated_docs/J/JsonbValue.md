# JsonbValue

## Location
[src/include/utils/jsonb.h:253-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/jsonb.h#L253-L296)

## Overview
JsonbValue is the in-memory representation of PostgreSQL's JSONB data type, providing a convenient deserialized structure for manipulating JSON data in memory.

## Definition


## Detailed Description
JsonbValue serves as PostgreSQL's in-memory representation for JSONB data, contrasting with the on-disk Jsonb format which has various alignment considerations. This structure provides a convenient deserialized representation that supports using the "val" union across different underlying types during manipulation. JsonbValues can either be shims through which a Jsonb buffer is accessed, or they can be deep copied and passed around independently.

The structure uses a discriminated union approach where the  field determines which member of the  union is active. This design enables efficient type-specific operations while maintaining a unified interface for all JSON value types.

## Parameters / Member Variables
### Primary Members:
- : An enum jbvType value that determines the JSON value type and influences sort order
- : A union containing type-specific data structures

### Union Members by Type:
- : Numeric data for jbvNumeric type
- : Boolean value for jbvBool type  
- : String data structure containing:
  - : Length of the string
  - : Character pointer (not necessarily null-terminated)
- : Array container structure containing:
  - : Number of elements in the array
  - : Pointer to array of JsonbValue elements
  - : Flag indicating if this is a top-level "raw scalar" array
- : Associative container structure containing:
  - : Number of key-value pairs (1 pair = 2 elements)
  - : Pointer to array of JsonbPair structures
- : Binary format structure containing:
  - : Length of the binary data
  - : Pointer to JsonbContainer in on-disk format
- : Date/time data structure containing:
  - : Datum containing the date/time value
  - : Type OID for the date/time type
  - : Type modifier
  - : Numeric time zone in seconds for TimestampTz data type

## Dependencies
- Types referenced:
  - jbvType (enum defining JSON value types)
  - Numeric (PostgreSQL numeric type)
  - [JsonbPair](JsonbPair.md) (key-value pair structure)
  - [JsonbContainer](JsonbContainer.md) (on-disk container format)
  - Datum (PostgreSQL datum type)
  - Oid (PostgreSQL object identifier type)
- Related structures:
  - Jsonb (on-disk representation)
  - [JsonbPair](JsonbPair.md) (object key-value pairs)
  - [JsonbContainer](JsonbContainer.md) (binary container format)

## Notes and Other Information
- The structure is designed for efficient in-memory JSON manipulation while the on-disk Jsonb format prioritizes storage efficiency and alignment
- The  field not only determines union member access but also influences sort order for JSON values
- String values in the structure are not necessarily null-terminated, requiring explicit length tracking
- The  flag in arrays indicates special handling for top-level scalar values stored as single-element arrays
- Binary format support allows direct access to on-disk Jsonb data without full deserialization
- Date/time support includes timezone information for TimestampTz types
- The structure supports PostgreSQL's full range of JSON types including nulls, booleans, numbers, strings, arrays, and objects