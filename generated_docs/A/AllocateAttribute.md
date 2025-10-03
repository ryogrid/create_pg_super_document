# AllocateAttribute

## Location
[src/backend/bootstrap/bootstrap.c:883-900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L883-L900)

## Overview
A static function that allocates and zero-initializes memory for a pg_attribute structure during PostgreSQL bootstrap operations.

## Definition

```c
static Form_pg_attribute
AllocateAttribute(void)
```
## Detailed Description
This function provides a simple memory allocation wrapper specifically designed for creating pg_attribute structures during bootstrap. It allocates exactly ATTRIBUTE_FIXED_PART_SIZE bytes in TopMemoryContext and zero-initializes the memory, returning a pointer cast to Form_pg_attribute.

The function is optimized for bootstrap scenarios where per-column ACLs are never set, so only the fixed-size portion of the pg_attribute structure is needed. This avoids the complexity and overhead of variable-length attribute storage that would be required for ACL information.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (allocates and zero-initializes memory)
  - ATTRIBUTE_FIXED_PART_SIZE (constant defining the size of fixed pg_attribute data)

- Called from:
  - [boot_openrel](../b/boot_openrel.md) (when opening relations during bootstrap)
  - [DefineAttr](../D/DefineAttr.md) (when defining new attributes during bootstrap)

## Notes and Other Information
- Uses TopMemoryContext to ensure the allocated attribute persists throughout bootstrap
- Zero-initialization ensures all fields start with safe default values
- Simplified allocation strategy since bootstrap never deals with per-column ACLs
- The ATTRIBUTE_FIXED_PART_SIZE constant excludes variable-length ACL data
- Essential for creating the in-memory representation of table schemas during bootstrap
- Memory is not explicitly freed since bootstrap is a short-lived process