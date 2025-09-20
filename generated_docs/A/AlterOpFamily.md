# AlterOpFamily

## Location
[src/backend/commands/opclasscmds.c:817-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L817-L880)

## Overview
Modifies an existing operator family by adding or removing operators and support functions, serving as the main entry point for ALTER OPERATOR FAMILY ADD/DROP commands.

## Definition

```c
Oid
AlterOpFamily(AlterOpFamilyStmt *stmt)
```
## Detailed Description
AlterOpFamily implements the ALTER OPERATOR FAMILY ... ADD/DROP SQL commands. It serves as a dispatcher that validates the access method and operator family, checks permissions, and then delegates the actual add or drop operations to specialized functions (AlterOpFamilyAdd or AlterOpFamilyDrop). 

The function retrieves access method properties needed for validation (such as maximum strategy and support function numbers), ensures the specified operator family exists, and enforces superuser privilege requirements before routing to the appropriate operation-specific handler.

This function specifically handles only ADD and DROP operations - other ALTER OPERATOR FAMILY commands like OWNER TO or RENAME go through different code paths.

## Parameters / Member Variables
- : Parsed ALTER OPERATOR FAMILY statement containing:
  - : Access method name
  - : List of names forming the operator family name
  - : Boolean indicating whether this is a DROP (true) or ADD (false) operation
  - : List of operators/functions to add or remove

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [get_opfamily_oid](../g/get_opfamily_oid.md)
  - superuser
  - [AlterOpFamilyDrop](AlterOpFamilyDrop.md)
  - [AlterOpFamilyAdd](AlterOpFamilyAdd.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)

## Notes and Other Information
- Requires superuser privileges for security reasons similar to DefineOpClass
- Acts as a dispatcher - the real work is done by AlterOpFamilyAdd and AlterOpFamilyDrop
- Validates access method existence and retrieves its operational parameters
- Returns the OID of the modified operator family
- Access method parameters (amstrategies, amsupport, amoptsprocnum) are used for validation in the delegated functions
- Other ALTER OPERATOR FAMILY operations (RENAME, OWNER TO, etc.) use different code paths and don't go through this function