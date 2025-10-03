# ResourceOwnerAddToHash

## Location
[src/backend/utils/resowner/resowner.c:237-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L237-L260)

## Overview
The  function adds a resource value of a given kind to the ResourceOwner's internal hash table, providing efficient storage and lookup for tracked resources.

## Definition

```c
static void
ResourceOwnerAddToHash(ResourceOwner owner, Datum value, const ResourceOwnerDesc *kind)
```
## Detailed Description
This function implements the insertion logic for the ResourceOwner's hash table using linear probing for collision resolution. When a resource needs to be added to the hash table, the function:

1. Computes the initial hash index using  and applies a bit mask to stay within the table bounds
2. Uses linear probing to find the first available slot, starting from the computed hash index
3. Stores the resource value and kind in the found slot
4. Increments the hash table's item count

The function uses open addressing with linear probing, which provides good cache locality and simple implementation. The capacity is always kept as a power of 2, allowing the use of bitwise AND (&) with a mask instead of expensive modulo operations for wrapping around the table.

## Parameters / Member Variables
- `owner`: Pointer to the ResourceOwner structure that contains the hash table where the resource will be added
- `value`: A Datum representing the resource value to be stored (typically a pointer or integer identifier)
- `*kind`: Pointer to ResourceOwnerDesc that identifies the type/category of resource being added
## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwner](ResourceOwner.md) (struct type)
  - [ResourceOwnerDesc](ResourceOwnerDesc.md) (struct type)  
  - [hash_resource_elem](../h/hash_resource_elem.md) (for computing initial hash index)
- Called from (representative examples):
  - [ResourceOwnerEnlarge](ResourceOwnerEnlarge.md) (during hash table expansion operations)

## Notes and Other Information
- This is a static function, only visible within the resowner.c compilation unit
- The function assumes the hash table has sufficient capacity; capacity management is handled by calling functions
- Uses linear probing for collision resolution, which provides good performance characteristics for the typical use patterns
- The hash table capacity is always a power of 2, enabling efficient modular arithmetic using bitwise operations
- The function includes an assertion to ensure the kind parameter is not NULL, helping catch programming errors early
- After insertion, the nhash counter is incremented to track the number of items in the hash table

## Simplified Source

```c
// Simplified version of ResourceOwnerAddToHash
static void ResourceOwnerAddToHash(ResourceOwner owner, Datum value, const ResourceOwnerDesc *kind) {
    // Validate input parameters
    Assert(kind != NULL);

    // Calculate hash table mask for efficient modulo operations
    uint32 mask = owner->capacity - 1;

    // Compute initial hash index for the resource
    uint32 idx = hash_resource_elem(value, kind) & mask;

    // Linear probing: find the first free slot
    while (owner->hash[idx].kind != NULL) {
        idx = (idx + 1) & mask;  // Wrap around using bitwise AND
    }

    // Store the resource in the found slot
    owner->hash[idx].item = value;
    owner->hash[idx].kind = kind;

    // Update the item count
    owner->nhash++;
}
```

Key simplifications made:
- Converted infinite for loop to a more readable while loop
- Added inline comments explaining the hash table mechanics
- Clarified the linear probing collision resolution strategy
- Explained the efficient modulo operation using bitwise AND
- Maintained all original logic while improving readability