# add_column_datatype_dependency

## Location
[src/backend/commands/tablecmds.c:7491-7508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7491-L7508)

## Overview
Establishes a dependency relationship between a table column and its data type to ensure referential integrity in the PostgreSQL catalog system.

## Definition
```c
static void add_column_datatype_dependency(Oid relid, int32 attnum, Oid typid)
```

## Detailed Description
This function creates a dependency entry in the PostgreSQL dependency system that links a specific column of a table to its data type. This dependency ensures that the data type cannot be dropped while the column still exists and uses that type. The dependency is recorded as a normal dependency (DEPENDENCY_NORMAL), meaning the column depends on the type for its existence.

The function constructs two ObjectAddress structures: one representing the column (identified by relation OID and attribute number) and another representing the data type (identified by type OID). It then calls recordDependencyOn to establish the dependency relationship.

## Parameters / Member Variables
- `relid`: The OID of the relation (table) containing the column
- `attnum`: The attribute number (column number) within the relation
- `typid`: The OID of the data type used by the column

## Dependencies
- Functions called/Symbols referenced:
  - [recordDependencyOn](../r/recordDependencyOn.md) (creates the dependency record in pg_depend)
  - DEPENDENCY_NORMAL (dependency type constant)
- Called from (representative examples):
  - [ATExecAddColumn](../A/ATExecAddColumn.md) (when adding new columns to tables)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md) (when changing column data types)

## Notes and Other Information
- The function is static, meaning it's only used within tablecmds.c
- Uses DEPENDENCY_NORMAL type, which means the dependent object (column) cannot exist without the referenced object (data type)
- The objectSubId for the data type is set to 0 since types don't have sub-components
- Part of PostgreSQL's dependency tracking system that prevents unsafe object deletions
- Essential for maintaining data integrity when types are involved in DDL operations

## Simplified Source

```c
static void add_column_datatype_dependency(Oid relid, int32 attnum, Oid typid) {
    ObjectAddress column_ref, type_ref;

    // Set up column reference (table + attribute number)
    column_ref.classId = RelationRelationId;
    column_ref.objectId = relid;
    column_ref.objectSubId = attnum;

    // Set up data type reference
    type_ref.classId = TypeRelationId;
    type_ref.objectId = typid;
    type_ref.objectSubId = 0;

    // Record dependency: column depends on its data type
    recordDependencyOn(&column_ref, &type_ref, DEPENDENCY_NORMAL);
}
```