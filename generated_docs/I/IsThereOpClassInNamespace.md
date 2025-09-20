# IsThereOpClassInNamespace

## Location
[src/backend/commands/opclasscmds.c:1805-1827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1805-L1827)

## Overview
Validates that an operator class with the specified name and access method does not already exist in the given namespace, raising an error if a duplicate is found.

## Definition

```c
void
IsThereOpClassInNamespace(const char *opcname, Oid opcmethod,
						  Oid opcnamespace)
```
## Detailed Description
This function serves as a validation subroutine for ALTER OPERATOR CLASS operations, specifically for SET SCHEMA and RENAME commands. It performs a namespace collision check by searching the system catalog to determine if an operator class with the specified name and access method already exists in the target namespace. If such a collision is detected, the function raises a comprehensive error message that includes the operator class name, access method name, and schema name to help users understand the conflict. This prevents naming conflicts and maintains the uniqueness constraints required for operator class identification within PostgreSQL's catalog system.

## Parameters / Member Variables
- : C string containing the name of the operator class to check for conflicts
- : Object identifier of the access method associated with the operator class
- : Object identifier of the namespace (schema) where the collision check should be performed

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists3
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - ereport
  - [get_am_name](../g/get_am_name.md)
  - [get_namespace_name](../g/get_namespace_name.md)
- Called from (representative examples):
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md) (src/backend/commands/alter.c:283)
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md) (src/backend/commands/alter.c:776)

## Notes and Other Information
- This is a non-static (public) function that can be called from other modules, as indicated by its declaration in defrem.h
- Uses the CLAAMNAMENSP system cache for efficient lookup of operator class entries by access method, name, and namespace
- The function only performs validation and does not return a value - it either completes successfully or raises an error
- Error reporting includes human-readable names for both the access method and namespace using get_am_name() and get_namespace_name()
- This function is part of PostgreSQL's DDL (Data Definition Language) infrastructure for maintaining catalog consistency
- The function name follows PostgreSQL's naming convention for existence-checking functions with the "IsThere" prefix