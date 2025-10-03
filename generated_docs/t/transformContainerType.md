# transformContainerType

## Location
[src/backend/parser/parse_node.c:189-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_node.c#L189-L242)

## Overview
A function that identifies and prepares the actual container type for subscripting operations by resolving domains to their base types and handling special vector types.

## Definition

```c
void
transformContainerType(Oid *containerType, int32 *containerTypmod)
```
## Detailed Description
This function prepares a container type for subscripting operations by performing several transformations:

1. **Domain Resolution**: If the input type is a domain, it resolves to the base type using getBaseTypeAndTypmod(). This is necessary because subscripting operations work on the underlying container type, not the domain wrapper.

2. **Vector Type Handling**: Treats special vector types (int2vector and oidvector) as their corresponding array types (int2[] and oid[]). This conversion is needed because array slicing operations could create arrays that don't satisfy the dimensionality constraints of the vector types, so the result should be considered the more general array type.

The function modifies the provided type OID and typmod in-place, allowing the caller to work with the actual subscribable container type.

## Parameters / Member Variables
- `*containerType`: Pointer to the OID of the container type to be transformed (modified in-place)
- `*containerTypmod`: Pointer to the type modifier of the container type (modified in-place if resolving a domain)
## Dependencies
- Functions called/Symbols referenced:
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)() (resolves domain types to base types)
  - INT2VECTOROID (type OID constant for int2vector)
  - INT2ARRAYOID (type OID constant for int2[])
  - OIDVECTOROID (type OID constant for oidvector) 
  - OIDARRAYOID (type OID constant for oid[])
- Called from (representative examples):
  - [transformContainerSubscripts](transformContainerSubscripts.md)() (processes subscript expressions)
  - [transformAssignmentSubscripts](transformAssignmentSubscripts.md)() (handles assignment to subscripted targets)

## Notes and Other Information
- The function works in-place, modifying the passed pointers rather than returning new values
- After calling this function, the caller still needs to verify that the result type is actually a container type that supports subscripting
- The domain-to-base-type conversion ensures that subscripting operations work consistently regardless of domain wrappers
- Special handling of vector types prevents type system inconsistencies when slicing operations create arrays with different dimensionality constraints
- Location: src/backend/parser/parse_node.c:189-242

## Simplified Source

```c
void transformContainerType(Oid *containerType, int32 *containerTypmod) {
    // Resolve domain to base type for subscripting operations
    *containerType = getBaseTypeAndTypmod(*containerType, containerTypmod);

    // Convert special vector types to corresponding array types
    // This allows slicing operations to work properly
    if (*containerType == INT2VECTOROID)
        *containerType = INT2ARRAYOID;
    else if (*containerType == OIDVECTOROID)
        *containerType = OIDARRAYOID;
}
```