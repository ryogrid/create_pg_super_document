# hash_resource_elem

## Location
[src/backend/utils/resowner/resowner.c:214-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L214-L236)

## Overview
The  function is an internal hash function that computes a hash value for a resource element based on its value and kind, used in PostgreSQL's resource ownership tracking system.

## Definition

```c
static inline uint32
hash_resource_elem(Datum value, const ResourceOwnerDesc *kind)
```
## Detailed Description
This function provides a hash function for value+kind combinations in the resource ownership system. The design philosophy accounts for the fact that most resource kinds store pointers in the 'value' parameter, which are naturally unique. However, some resources store plain integers (such as Files and Buffers), which could cause hash collisions if only the value were used.

To address this, the function incorporates both the resource value and the resource kind pointer into the hash calculation. The implementation uses conditional compilation based on the system's Datum size:
- For 64-bit systems (SIZEOF_DATUM == 8): Uses  for the value and  to combine it with the kind pointer
- For 32-bit systems: Uses  for the value and  to combine it with the kind pointer

The approach is deliberately lightweight since there are only a few resource kinds that store integers, making sophisticated mixing unnecessary.

## Parameters / Member Variables
- : A Datum representing the resource value (typically a pointer, but can be an integer for some resource types like Files and Buffers)
- : A pointer to ResourceOwnerDesc that identifies the type/kind of resource being hashed

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerDesc](../R/ResourceOwnerDesc.md) (struct type)
  - SIZEOF_DATUM (macro)
  - [hash_combine64](hash_combine64.md) (64-bit systems)
  - [murmurhash64](../m/murmurhash64.md) (64-bit systems)
  - hash_combine (32-bit systems)
  - [murmurhash32](../m/murmurhash32.md) (32-bit systems)
- Called from (representative examples):
  - [ResourceOwnerAddToHash](../R/ResourceOwnerAddToHash.md)
  - [ResourceOwnerForget](../R/ResourceOwnerForget.md)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the resowner.c compilation unit and is likely to be inlined by the compiler for performance
- The hash function is specifically designed for the resource owner's internal hash table implementation
- The choice of hash function depends on the system's pointer size, ensuring optimal performance on both 32-bit and 64-bit architectures
- The function balances simplicity with effectiveness, avoiding over-engineering since resource kind collisions are rare