# AlterSchemaOwner

## Location
[src/backend/commands/schemacmds.c:330-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/schemacmds.c#L330-L360)

## Overview
AlterSchemaOwner implements the ALTER SCHEMA OWNER operation for schemas identified by name, changing schema ownership through the internal ownership change mechanism.

## Definition


## Detailed Description
AlterSchemaOwner provides the primary interface for changing schema ownership when the schema is identified by name rather than OID. This function handles the catalog lookup, locking, and delegates the actual ownership change to AlterSchemaOwner_internal. It serves as the main entry point for SQL ALTER SCHEMA ... OWNER TO commands, performing necessary validation and setup before invoking the internal ownership change logic.

Key behaviors include:
- Looking up the schema by name in the pg_namespace system catalog
- Opening the catalog relation with appropriate locking for modification
- Extracting the schema OID from the catalog tuple
- Delegating to AlterSchemaOwner_internal for the actual ownership change
- Returning an ObjectAddress for the modified schema
- Proper cleanup of system cache references and relation locks

## Parameters / Member Variables
- : Name of the schema whose ownership should be changed
- : OID of the role that should become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_close (catalog access with RowExclusiveLock)
  - [SearchSysCache1](../S/SearchSysCache1.md)/ReleaseSysCache (schema lookup by name)
  - [AlterSchemaOwner_internal](AlterSchemaOwner_internal.md) (performs the actual ownership change)
  - ObjectAddressSet (constructs return value)
- Called from (representative examples):
  - [ExecAlterOwnerStmt](../E/ExecAlterOwnerStmt.md) (general ALTER OWNER statement execution)

## Notes and Other Information
- Returns ObjectAddress identifying the schema that was modified
- Uses RowExclusiveLock on NamespaceRelationId to ensure exclusive access during ownership change
- Provides error handling for non-existent schemas with appropriate error codes
- Serves as the name-based interface to the schema ownership change infrastructure
- Part of the standard ALTER OWNER command processing pipeline