# _equalExtensibleNode

## Location
[src/backend/nodes/equalfuncs.c:117-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/equalfuncs.c#L117-L133)

## Overview
A static comparison function that determines if two ExtensibleNode instances are equal by first comparing their extension names and then delegating to the extension-specific equality function.

## Definition

```c
static bool
_equalExtensibleNode(const ExtensibleNode *a, const ExtensibleNode *b)
```
## Detailed Description
The  function provides equality comparison for PostgreSQL's extensible node framework. Extensible nodes allow extensions to define custom node types that integrate seamlessly with PostgreSQL's node infrastructure. 

The function performs a two-stage comparison: first it verifies that both nodes have the same extension name (), and then it delegates to the extension-specific equality function through the  interface. This design allows each extension to define its own comparison logic for its private data while maintaining consistency with PostgreSQL's node system.

## Parameters / Member Variables
- `*a`: Pointer to the first ExtensibleNode to compare
- `*b`: Pointer to the second ExtensibleNode to compare
## Dependencies
- Functions called/Symbols referenced:
  -  (macro for comparing string fields)
  -  (retrieves method structure for extensible node type)
  -  (extension-specific equality function)
- Called from (representative examples):
  - [Node](../N/Node.md) equality framework (indirectly through function pointers)

## Notes and Other Information
- This function is marked as , meaning it's only accessible within the equalfuncs.c file
- Part of PostgreSQL's extensible node framework, allowing extensions to define custom node types
- The function first ensures both nodes have the same  before proceeding with detailed comparison
- Delegates actual comparison logic to the extension-provided  method, enabling custom comparison semantics
- Uses the established pattern of  for consistent field comparison
- The ExtensibleNode structure has the  attribute, indicating custom comparison logic
- This design maintains type safety while allowing extensions to extend PostgreSQL's node system with their own data structures and comparison logic