# recordDependencyOnOwner

## Location
[src/backend/catalog/pg_shdepend.c:168-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L168-L205)

## Overview
A convenience wrapper function that records ownership dependency between a database object and its owner (user/role) by creating an entry in the shared dependency system.

## Definition

```c
void
recordDependencyOnOwner(Oid classId, Oid objectId, Oid owner)
```
## Detailed Description
This function simplifies the process of recording ownership relationships in PostgreSQL. It constructs ObjectAddress structures for both the dependent object and the owner, then calls recordSharedDependencyOn with SHARED_DEPENDENCY_OWNER type to establish the ownership relationship. The owner is always referenced from the pg_authid catalog (AuthIdRelationId). This function is widely used throughout the system when creating objects that have owners.

## Parameters / Member Variables
- : OID of the system catalog that contains the dependent object
- : OID of the dependent object within its catalog
- : OID of the owner (user/role) from pg_authid catalog

## Dependencies
- Functions called/Symbols referenced:
  - [recordSharedDependencyOn](recordSharedDependencyOn.md)
  - SHARED_DEPENDENCY_OWNER (dependency type constant)
- Called from (representative examples):
  - [createdb](../c/createdb.md)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - [CollationCreate](../C/CollationCreate.md)
  - [ConversionCreate](../C/ConversionCreate.md)
  - [NamespaceCreate](../N/NamespaceCreate.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [CreateTableSpace](../C/CreateTableSpace.md)
  - [CreateSubscription](../C/CreateSubscription.md)
  - [CreatePublication](../C/CreatePublication.md)
  - [CreateForeignDataWrapper](../C/CreateForeignDataWrapper.md)

## Notes and Other Information
- It's the caller's responsibility to ensure no owner entry already exists for the object
- Both object addresses have objectSubId set to 0 (no sub-objects for ownership dependencies)
- The owner is always referenced through AuthIdRelationId (pg_authid catalog)
- This function is essential for PostgreSQL's object ownership and privilege system
- Located in src/backend/catalog/pg_shdepend.c:168-205

## Simplified Source

```c
void recordDependencyOnOwner(Oid classId, Oid objectId, Oid owner)
{
    ObjectAddress myself, referenced;

    // Set up the dependent object address
    myself.classId = classId;
    myself.objectId = objectId;
    myself.objectSubId = 0;

    // Set up the owner object address (always from pg_authid)
    referenced.classId = AuthIdRelationId;
    referenced.objectId = owner;
    referenced.objectSubId = 0;

    // Record the ownership dependency
    recordSharedDependencyOn(&myself, &referenced, SHARED_DEPENDENCY_OWNER);
}
```