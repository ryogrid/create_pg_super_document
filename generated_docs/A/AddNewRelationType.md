# AddNewRelationType

## Location
src/backend/catalog/heap.c: 1027 - 1104

## Overview
Defines a composite type corresponding to a new relation by creating an appropriate type entry in the pg_type system catalog.

## Definition


## Detailed Description
AddNewRelationType is a specialized wrapper around TypeCreate that creates a composite type entry corresponding to a new relation. In PostgreSQL, every table has an associated composite type that represents the row type of that table, allowing tuples to be used as values in other contexts.

The function calls TypeCreate with carefully chosen parameters appropriate for composite types. It sets the type category to TYPTYPE_COMPOSITE, uses standard record input/output functions (record_in, record_out, record_recv, record_send), and configures storage and alignment properties suitable for composite types.

Key characteristics of the created type include: variable-length internal storage (-1 size), TYPALIGN_DOUBLE alignment for maximum compatibility, TYPSTORAGE_EXTENDED for full TOAST capability, and no default values or special type modification functions. The function returns an ObjectAddress identifying the newly created type.

## Parameters / Member Variables
- : Name for the composite type being created
- : Namespace OID where the type should be created
- : OID of the relation this type corresponds to
- : Kind of relation (table, view, etc.) this type represents
- : OID of the user who will own this type
- : Predetermined OID for the row type being created (may be InvalidOid for auto-assignment)
- : OID for the corresponding array type (may be InvalidOid)

## Dependencies
- Functions called/Symbols referenced:
  - [TypeCreate](../T/TypeCreate.md)
  - TYPTYPE_COMPOSITE
  - TYPCATEGORY_COMPOSITE
  - DEFAULT_TYPDELIM
  - TYPALIGN_DOUBLE
  - TYPSTORAGE_EXTENDED
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)

## Notes and Other Information
- This function is static and primarily used during relation creation as part of heap_create_with_catalog
- Every PostgreSQL table automatically gets a corresponding composite type that represents its row structure
- The created type uses standard record I/O functions, making it compatible with generic record operations
- Composite types are never marked as "preferred" in type resolution contexts
- The type is created with maximum alignment (TYPALIGN_DOUBLE) to ensure compatibility with all possible attribute types
- TYPSTORAGE_EXTENDED enables full TOAST support for large composite values
- The function returns an ObjectAddress that can be used for dependency tracking and other catalog operations