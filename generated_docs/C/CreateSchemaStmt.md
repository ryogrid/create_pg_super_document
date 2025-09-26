# CreateSchemaStmt

## Location
[src/include/nodes/parsenodes.h:2320-2327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2320-L2327)

## Overview
CreateSchemaStmt represents a CREATE SCHEMA statement that creates a new schema namespace and optionally executes embedded SQL statements within that schema context.

## Definition
```c
typedef struct CreateSchemaStmt
{
    NodeTag     type;
    char       *schemaname;      /* the name of the schema to create */
    RoleSpec   *authrole;        /* the owner of the created schema */
    List       *schemaElts;      /* schema components (list of parsenodes) */
    bool        if_not_exists;   /* just do nothing if schema already exists? */
} CreateSchemaStmt;
```

## Detailed Description
CreateSchemaStmt represents the SQL CREATE SCHEMA command, which creates a new schema namespace in the database. The schema can optionally contain a list of SQL statements (CREATE TABLE, GRANT, etc.) that are executed after the schema is created. These embedded statements are analyzed and executed in sequence with the new schema temporarily prepended to the search path.

The creation process involves several security checks including schema-create privilege on the database and the ability to assume the target owner role. Reserved schema names (those starting with "pg_") are protected unless system table modifications are allowed. The IF NOT EXISTS clause allows graceful handling of pre-existing schemas.

During execution, the new schema is temporarily added to the search_path, allowing embedded statements to reference objects without qualification. The embedded statements are reorganized into a sequentially executable order with no forward references before execution.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreateSchemaStmt node
- `schemaname`: Name of the schema to create (NULL means use current user's name)
- `authrole`: RoleSpec specifying the owner of the schema (NULL means current user)
- `schemaElts`: List of raw parse trees for statements to execute within the schema context
- `if_not_exists`: Boolean flag indicating whether to skip creation if schema already exists

## Dependencies
- Functions called/Symbols referenced:
  - [RoleSpec](../R/RoleSpec.md) (for specifying schema ownership)
  - NodeTag (inherited node type system)
  - [List](../L/List.md) (for schema elements)
- Called from (representative examples):
  - [CreateSchemaCommand](CreateSchemaCommand.md) (main execution function)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)
  - [CreateExtensionInternal](CreateExtensionInternal.md) (during extension creation)

## Notes and Other Information
- Schema elements are raw parse trees that are analyzed and executed after schema creation
- The new schema is temporarily prepended to search_path during element execution
- Reserved names (pg_*) are rejected unless allowSystemTableMods is enabled
- Requires both CREATE privilege on database and ability to assume target role
- IF NOT EXISTS provides idempotent schema creation behavior
- Schema elements are reordered to resolve forward references before execution
- Event triggers are notified of schema creation before processing embedded elements