# AlterDefaultPrivilegesStmt

## Location
src/include/nodes/parsenodes.h: 2571 - 2576

## Overview
AlterDefaultPrivilegesStmt is a parse tree node structure that represents SQL ALTER DEFAULT PRIVILEGES statements, used to modify default privileges for future database objects.

## Definition
```c
typedef struct AlterDefaultPrivilegesStmt
{
    NodeTag    type;
    List      *options;        /* list of DefElem */
    GrantStmt *action;         /* GRANT/REVOKE action (with objects=NIL) */
} AlterDefaultPrivilegesStmt;
```

## Detailed Description
AlterDefaultPrivilegesStmt represents the parsed form of ALTER DEFAULT PRIVILEGES SQL statements. These statements allow users to set default privileges that will be automatically applied to objects created in the future by a particular role or in a particular schema. The structure encapsulates both the context options (such as target schemas or roles) and the privilege action to be applied as defaults.

The action field contains a GrantStmt structure but with its objects list set to NIL, since default privileges apply to future objects rather than existing ones.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AlterDefaultPrivilegesStmt node in the parse tree
- `options`: List of DefElem nodes specifying context options such as IN SCHEMA, FOR ROLE, etc.
- `action`: GrantStmt structure containing the GRANT or REVOKE action to be applied as default (objects field should be NIL)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (parse tree node identification)
  - List (PostgreSQL list data structure)
  - GrantStmt (grant/revoke statement structure)
  - DefElem (definition element for options)

- Called from (representative examples):
  - ExecAlterDefaultPrivilegesStmt
  - EventTriggerCollectAlterDefPrivs
  - ProcessUtilitySlow

## Notes and Other Information
- Default privileges only apply to objects created after the ALTER DEFAULT PRIVILEGES statement is executed
- The options list can contain specifications like IN SCHEMA schema_name or FOR ROLE role_name
- The embedded GrantStmt structure reuses existing grant/revoke parsing logic but with empty object lists
- This mechanism provides a way to establish consistent privilege patterns for new objects without requiring manual grants after each object creation
- Event triggers can capture these statements for auditing or replication purposes