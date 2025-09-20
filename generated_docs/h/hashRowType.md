# hashRowType

## Location
[src/backend/access/common/tupdesc.c:622-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L622-L650)

## Overview
hashRowType generates a hash value for a TupleDesc that is consistent with the equality semantics of equalRowTypes, enabling efficient hash-based lookups and storage of row type descriptors.

## Definition

```c
structure in
 *		a previously allocated tuple descriptor.
 *
 * If attributeName is NULL, the attname field is set to an empty string
 * (this is for cases where we don't know or need a name for the field).
 * Also, some callers use this function to change the datatype-related fields
 * in an existing tupdesc;
```
## Detailed Description
This function computes a hash value for a TupleDesc structure using the same fields that  considers for equality comparison. The hash computation ensures that two TupleDescs that would be considered equal by  will produce identical hash values, making it suitable for use in hash tables and caches.

The hash value is computed by combining:
1. **Number of attributes** () - ensures descriptors with different column counts hash differently
2. **Composite type ID** () - distinguishes between different composite types
3. **Attribute type IDs** () for each attribute - captures the essential type information

The function uses PostgreSQL's standard hash combining functions ( and ) to create a well-distributed hash value. Notably, it hashes only the most fundamental identifying characteristics of the row type, excluding detailed attribute properties like names, type modifiers, and collations that  checks.

## Parameters / Member Variables
- : The TupleDesc whose row type should be hashed

## Dependencies
- Functions called/Symbols referenced:
  - [hash_uint32](hash_uint32.md) (hashes individual 32-bit values)
  - hash_combine (combines hash values into a composite hash)
- Called from (representative examples):
  - [shared_record_table_hash](../s/shared_record_table_hash.md) (for type cache hash table operations)
  - [record_type_typmod_hash](../r/record_type_typmod_hash.md) (for type modifier hash operations)
  - ReleaseTupleDesc (in release optimization logic)

## Notes and Other Information
- Designed to be consistent with  equality semantics
- Uses a simplified hash computation that focuses on structural essentials rather than complete attribute details
- Essential for efficient type cache lookups and record type management
- The hash includes only , , and  fields - notably excludes attribute names, type modifiers, and collations
- Part of PostgreSQL's type caching infrastructure for performance optimization