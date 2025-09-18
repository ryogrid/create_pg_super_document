# recordDependencyOnOwner

## Location
src/backend/catalog/pg_shdepend.c: 168 - 205

## Overview
A convenience wrapper function that records ownership dependency between a database object and its owner (user/role) by creating an entry in the shared dependency system.

## Definition


## Detailed Description
This function simplifies the process of recording ownership relationships in PostgreSQL. It constructs ObjectAddress structures for both the dependent object and the owner, then calls recordSharedDependencyOn with SHARED_DEPENDENCY_OWNER type to establish the ownership relationship. The owner is always referenced from the pg_authid catalog (AuthIdRelationId). This function is widely used throughout the system when creating objects that have owners.

## Parameters / Member Variables
- : OID of the system catalog that contains the dependent object
- : OID of the dependent object within its catalog
- : OID of the owner (user/role) from pg_authid catalog

## Dependencies
- Functions called/Symbols referenced:
  - recordSharedDependencyOn
  - SHARED_DEPENDENCY_OWNER (dependency type constant)
- Called from (representative examples):
  - createdb
  - heap_create_with_catalog
  - CollationCreate
  - ConversionCreate
  - NamespaceCreate
  - ProcedureCreate
  - CreateTableSpace
  - CreateSubscription
  - CreatePublication
  - CreateForeignDataWrapper

## Notes and Other Information
- It's the caller's responsibility to ensure no owner entry already exists for the object
- Both object addresses have objectSubId set to 0 (no sub-objects for ownership dependencies)
- The owner is always referenced through AuthIdRelationId (pg_authid catalog)
- This function is essential for PostgreSQL's object ownership and privilege system
- Located in src/backend/catalog/pg_shdepend.c:168-205