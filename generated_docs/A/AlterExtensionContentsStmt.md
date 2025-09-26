# AlterExtensionContentsStmt

## Location
src/include/nodes/parsenodes.h: 2835 - 2842

## Overview
AlterExtensionContentsStmt represents the parsed structure for an ALTER EXTENSION ADD/DROP statement, used to dynamically add or remove database objects from an extension's membership.

## Definition


## Detailed Description
This structure represents the ALTER EXTENSION ... ADD/DROP commands that allow database administrators to dynamically modify extension membership after installation. Objects can be added to make them part of an extension (making them dependent on the extension and subject to being dropped when the extension is dropped), or removed to make them standalone database objects again.

The operation involves managing dependency records in pg_depend, handling initial ACL records in pg_init_privs, and potentially updating extension configuration arrays. The system automatically handles dependent objects like array types for base types, multirange types for range types, and row types for tables.

## Parameters / Member Variables
- : NodeTag identifier indicating this is an AlterExtensionContentsStmt node
- : Name of the extension to modify
- : Numeric action indicator (+1 for ADD operations, -1 for DROP operations)
- : Type of the database object being added/removed (from ObjectType enum)
- : Parse tree node representing the qualified name of the target object

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enumeration defining object types)
  - NodeTag (from node system)
  - Node (generic parse tree node)
- Called from (representative examples):
  - ExecAlterExtensionContentsStmt (main execution function)
  - ExecAlterExtensionContentsRecurse (recursive helper function)
  - ProcessUtilitySlow (utility command processor)

## Notes and Other Information
- Certain object types cannot be added to extensions (databases, other extensions, indexes, publications, roles, statistics, subscriptions, tablespaces)
- ADD operations prevent adding objects that are already members of any extension
- DROP operations verify the object is actually a member of the specified extension
- Permission checks require ownership of both the extension and the target object
- The operation prevents circular dependencies (e.g., adding a schema to an extension when the schema contains the extension)
- Automatically handles dependent objects recursively (array types, multirange types, row types)
- Manages both dependency records and initial privilege records for proper extension behavior
- For relations, may also update the extension's configuration array (extconfig)