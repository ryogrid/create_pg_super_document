# heap_create_with_catalog

## Location
[src/backend/catalog/heap.c:1105-1525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1105-L1525)

## Overview
Creates a new relation with a complete catalog entry in PostgreSQL's system catalogs, handling both the physical storage creation and all associated metadata registration.

## Definition

```c
enumber = InvalidRelFileNumber;
```
## Detailed Description
This function is the comprehensive interface for creating new relations in PostgreSQL. It handles the complete process of creating a relation from start to finish, including:

1. **Validation and Conflict Checking**: Validates the tuple descriptor, checks for existing relations and types with conflicting names, and handles array type name conflicts through moveArrayTypeName.

2. **OID Assignment**: Allocates OIDs for the relation and handles binary upgrade scenarios where specific OIDs may be pre-assigned.

3. **Physical Storage Creation**: Creates the actual heap storage through heap_create() and establishes the relcache entry.

4. **Type System Integration**: For most relation kinds (excluding sequences, toast tables, and indexes), creates corresponding PostgreSQL composite types and array types through AddNewRelationType and TypeCreate.

5. **Catalog Registration**: Registers the relation in pg_class via AddNewRelationTuple and creates attribute entries in pg_attribute via AddNewAttributeTuples.

6. **Dependency Management**: Establishes proper dependency relationships with namespaces, owners, access methods, and extensions to ensure proper cascade behavior during drops.

7. **Constraint and Default Handling**: Processes any supplied constraints and defaults through StoreConstraints.

8. **Transaction Coordination**: Handles special commit actions for temporary tables and ensures proper locking throughout the process.

## Parameters / Member Variables
- : Name of the new relation
- : OID of the namespace where the relation will be created
- : OID of the tablespace for physical storage
- : Specific OID to assign to the relation, or InvalidOid for automatic assignment
- : OID for the relation's row type, or InvalidOid for automatic assignment
- : For typed tables, the OID of the underlying composite type
- : OID of the relation's owner (user/role)
- : OID of the access method to use for the relation
- : Tuple descriptor defining the relation's column structure
- : List of pre-processed CHECK constraints and column defaults
- : Character indicating the kind of relation (table, view, index, etc.)
- : Persistence characteristic (permanent, temporary, or unlogged)
- : Whether this is a shared system relation
- : Whether the relation uses the relfilenumber mapping system
- : ON COMMIT action for temporary tables
- : Relation options in Datum form
- : Whether to apply user-defined default ACL permissions
- : Whether to allow creation in system namespaces
- : Whether this is an internal system-generated relation
- : OID used for tracking relation rewrites
- : Output parameter receiving the object address of the created pg_type entry

## Dependencies
- Functions called/Symbols referenced:
  - [heap_create](heap_create.md) (creates physical storage and relcache entry)
  - [AddNewRelationType](../A/AddNewRelationType.md) (creates composite type for relation)
  - [TypeCreate](../T/TypeCreate.md) (creates array type over composite type)
  - [AddNewRelationTuple](../A/AddNewRelationTuple.md) (registers relation in pg_class)
  - [AddNewAttributeTuples](../A/AddNewAttributeTuples.md) (creates pg_attribute entries)
  - [moveArrayTypeName](../m/moveArrayTypeName.md) (handles array type name conflicts)
  - [StoreConstraints](../S/StoreConstraints.md) (processes constraints and defaults)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md) (establishes ownership dependencies)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md) (handles extension membership)
  - [register_on_commit_action](../r/register_on_commit_action.md) (handles temporary table commit actions)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (main table creation path)
  - [create_toast_table](../c/create_toast_table.md) (TOAST table creation)
  - [make_new_heap](../m/make_new_heap.md) (table clustering/rewriting)

## Notes and Other Information
- This function is central to PostgreSQL's DDL operations and is used whenever a new cataloged relation needs to be created
- The function handles binary upgrade scenarios specially, using pre-assigned OIDs to maintain consistency during pg_upgrade operations
- For relations that get composite types (most relations except sequences, toast tables, and indexes), both a composite type and its corresponding array type are created
- The function establishes comprehensive dependency tracking to ensure proper cleanup during DROP operations
- Access method dependencies are only recorded for relations that actually use table access methods
- The function requires either normal processing mode or bootstrap mode - it cannot be called during other initialization phases
- Lock acquisition on the new relation OID prevents race conditions with concurrent DDL operations