# ObjectAddress

## Location
[src/include/catalog/objectaddress.h:24-29](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/objectaddress.h#L24-L29)

## Overview
ObjectAddress is a fundamental data structure that represents any database object in PostgreSQL, providing a unified way to identify tables, functions, types, constraints, and other database entities through a three-component addressing scheme.

## Definition
```c
typedef struct ObjectAddress
{
    Oid         classId;        /* Class Id from pg_class */
    Oid         objectId;       /* OID of the object */
    int32       objectSubId;    /* Subitem within object (eg column), or 0 */
} ObjectAddress;
```

## Detailed Description
ObjectAddress serves as PostgreSQL's universal object identifier, enabling the system to reference any database object in a consistent manner. This structure is essential for dependency tracking, permission checking, object manipulation, and catalog operations. The three-field design allows for hierarchical addressing where objects can have sub-objects (like columns within tables).

The structure is extensively used throughout PostgreSQL's internal systems, particularly in:
- Dependency management and tracking
- Object ownership and permission verification  
- DDL operations and object manipulation
- Catalog system operations
- Object description and identity resolution

## Parameters / Member Variables
- `classId`: The OID of the system catalog that contains this object (corresponds to pg_class entries for different object types)
- `objectId`: The unique OID of the specific object within its catalog
- `objectSubId`: Sub-object identifier within the main object (e.g., column number for table attributes), or 0 for the object itself

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a pure data structure)
- Used extensively by:
  - [get_object_address](../g/get_object_address.md)
  - [get_object_address_rv](../g/get_object_address_rv.md)
  - [check_object_ownership](../c/check_object_ownership.md)
  - [get_object_namespace](../g/get_object_namespace.md)
  - [getObjectDescription](../g/getObjectDescription.md)
  - [getObjectDescriptionOids](../g/getObjectDescriptionOids.md)
  - getObjectTypeDescription
  - [getObjectIdentity](../g/getObjectIdentity.md)
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md)

## Notes and Other Information
- An InvalidObjectAddress constant is provided for initialization and error conditions
- Helper macros ObjectAddressSet and ObjectAddressSubSet are available for convenient initialization
- The classId typically corresponds to system catalog relation OIDs (e.g., RelationRelationId, ProcedureRelationId)
- A zero objectSubId indicates the entire object; non-zero values indicate specific sub-components
- This structure is central to PostgreSQL's object dependency system and is used throughout the codebase for object management operations
- The design allows PostgreSQL to maintain referential integrity and implement features like CASCADE operations across different object types