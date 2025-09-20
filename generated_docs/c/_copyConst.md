# _copyConst

## Location
[src/backend/nodes/copyfuncs.c:73-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/copyfuncs.c#L73-L107)

## Overview
Creates a deep copy of a Const node, handling both value and reference types appropriately based on the constant's properties.

## Definition

```c
struct when value is null!
		 */
		newnode->constvalue = from->constvalue;
```
## Detailed Description
The  function is a specialized copy function for Const nodes in PostgreSQL's parse tree. It performs a deep copy by creating a new Const node and copying all scalar fields, with special handling for the constant value itself. The function intelligently handles both pass-by-value and pass-by-reference data types, ensuring that referenced data is properly duplicated rather than just copying pointers.

For pass-by-value types or null constants, it simply copies the datum value directly. For pass-by-reference types, it uses  to create a properly allocated copy of the referenced data, preventing issues with shared references between the original and copied nodes.

## Parameters / Member Variables
- : Pointer to the source Const node to be copied

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create new Const node)
  - COPY_SCALAR_FIELD (macro for copying scalar fields)
  - [datumCopy](../d/datumCopy.md) (for deep copying pass-by-reference values)  
  - COPY_LOCATION_FIELD (macro for copying location information)
- Called from (representative examples):
  - Part of the node copying system (called indirectly through copyObject)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the copyfuncs.c file
- The function has custom logic to handle PostgreSQL's dual nature of pass-by-value vs pass-by-reference data types
- It's part of PostgreSQL's generic node copying infrastructure, used when duplicating parse trees
- The function is careful to avoid copying data when the constant is null to prevent accessing invalid memory