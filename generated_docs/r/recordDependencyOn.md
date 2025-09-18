# recordDependencyOn

## Location
src/backend/catalog/pg_depend.c: 46 - 57

## Overview
Records a dependency between two database objects via their respective ObjectAddress structures, creating an entry in the pg_depend catalog table.

## Definition


## Detailed Description
This function provides a simple interface for recording a dependency relationship between two PostgreSQL database objects. It serves as a wrapper around  for the common case of recording a single dependency. The function creates an entry in the pg_depend system catalog table without performing any additional processing or validation. The dependency relationship indicates that the depender object relies on the referenced object and helps PostgreSQL's dependency tracking system manage object lifecycles, particularly during DROP operations.

## Parameters / Member Variables
- : Pointer to ObjectAddress of the dependent object (the one that depends on another)
- : Pointer to ObjectAddress of the referenced object (the one being depended upon)
- : DependencyType enum value specifying the type of dependency relationship (e.g., DEPENDENCY_NORMAL, DEPENDENCY_AUTO, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - recordMultipleDependencies
  - DependencyType
- Called from (representative examples):
  - SetDefaultACL
  - AddNewAttributeTuples
  - index_create
  - CollationCreate
  - ConversionCreate
  - recordDependencyOnCurrentExtension
  - publication_add_relation
  - CreateAccessMethod
  - CreateForeignDataWrapper
  - CreateTriggerFiringOn

## Notes and Other Information
- This is a convenience function that internally calls  with a count of 1
- Located in src/backend/catalog/pg_depend.c:46-57
- Does not perform any validation or additional processing beyond creating the dependency record
- The dependency type determines how PostgreSQL handles the dependency during object deletion operations
- Part of PostgreSQL's object dependency tracking infrastructure used throughout the system for maintaining referential integrity