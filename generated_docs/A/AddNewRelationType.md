# AddNewRelationType

## Location
[src/backend/catalog/heap.c:1027-1104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1027-L1104)

## Overview
Defines a composite type corresponding to a new relation by creating an appropriate type entry in the pg_type system catalog.

## Definition

```c
enumber map
 *	oncommit: ON COMMIT marking (only relevant if it's a temp table)
 *	reloptions: reloptions in Datum form, or (Datum) 0 if none
 *	use_user_acl: true if should look for user-defined default permissions;
```
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

## Simplified Source

```c
static ObjectAddress
AddNewRelationType(const char *typeName,
                   Oid typeNamespace,
                   Oid new_rel_oid,
                   char new_rel_kind,
                   Oid ownerid,
                   Oid new_row_type,
                   Oid new_array_type) {

    // Create composite type for the relation using TypeCreate
    return TypeCreate(new_row_type,           // Optional predetermined OID
                     typeName,               // Type name
                     typeNamespace,          // Type namespace
                     new_rel_oid,           // Relation OID
                     new_rel_kind,          // Relation kind
                     ownerid,               // Owner's ID
                     -1,                    // Internal size (varlena)
                     TYPTYPE_COMPOSITE,     // Type category (composite)
                     TYPCATEGORY_COMPOSITE, // Type category
                     false,                 // Not preferred type
                     DEFAULT_TYPDELIM,      // Default array delimiter
                     F_RECORD_IN,           // Input procedure
                     F_RECORD_OUT,          // Output procedure
                     F_RECORD_RECV,         // Receive procedure
                     F_RECORD_SEND,         // Send procedure
                     InvalidOid,            // No typmod input
                     InvalidOid,            // No typmod output
                     InvalidOid,            // Default analyze procedure
                     InvalidOid,            // No subscript procedure
                     InvalidOid,            // No array element type
                     false,                 // Not an array type
                     new_array_type,        // Array type if any
                     InvalidOid,            // No domain base type
                     NULL,                  // No default value
                     NULL,                  // No default binary representation
                     false,                 // Passed by reference
                     TYPALIGN_DOUBLE,       // Maximum alignment
                     TYPSTORAGE_EXTENDED,   // Full TOAST capability
                     -1,                    // No typmod
                     0,                     // No array dimensions
                     false,                 // Type NOT NULL
                     InvalidOid);           // No collation for rowtypes
}
```