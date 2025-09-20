# processTypesSpec

## Location
[src/backend/commands/opclasscmds.c:1108-1136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1108-L1136)

## Overview
Processes explicit argument types used in ALTER ADD/DROP operator family commands, extracting and validating type specifications from a list of arguments.

## Definition

```c
static void
processTypesSpec(List *args, Oid *lefttype, Oid *righttype)
```
## Detailed Description
This static function parses a list of type name arguments and converts them to their corresponding OIDs for use in operator family operations. It handles both unary and binary operators by extracting one or two type specifications. For unary operators (single argument), it sets both left and right types to the same value. For binary operators (two arguments), it processes each type separately. The function enforces that no more than two argument types can be specified.

## Parameters / Member Variables
- : List of TypeName nodes representing the argument types to process
- : Output parameter - pointer to store the OID of the left operand type
- : Output parameter - pointer to store the OID of the right operand type

## Dependencies
- Functions called/Symbols referenced:
  - [TypeName](../T/TypeName.md) (type)
  - linitial
  - [typenameTypeId](../t/typenameTypeId.md)
  - list_length
  - lsecond
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md)
  - [AlterOpFamilyDrop](../A/AlterOpFamilyDrop.md)

## Notes and Other Information
- This function is specifically designed for ALTER operator family commands
- Validates that exactly 1 or 2 argument types are provided, throwing a syntax error for more than 2
- For single argument cases (unary operators), both lefttype and righttype are set to the same OID
- Uses PostgreSQL's type resolution system via typenameTypeId to convert type names to OIDs
- Part of the operator class/family management subsystem in PostgreSQL