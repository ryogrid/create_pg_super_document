# typenameType

## Location
[src/backend/parser/parse_type.c:264-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L264-L290)

## Overview
typenameType provides a safe, validated interface for type name resolution that guarantees the returned type is fully defined and valid for use.

## Definition
```c
Type typenameType(ParseState *pstate, const TypeName *typeName, int32 *typmod_p)
```

## Detailed Description
typenameType is the recommended high-level interface for converting TypeName objects to Type structures in PostgreSQL's parser. Unlike LookupTypeName, this function performs additional validation to ensure the resolved type is fully defined (not just a shell type) and always raises appropriate errors for missing or undefined types. This makes it safe for callers to assume the returned Type represents a complete, usable type definition. The function is commonly used in DDL operations and type conversions where type validity is critical.

## Parameters / Member Variables
- `pstate`: ParseState pointer for error location reporting and context
- `typeName`: TypeName structure containing the type specification to resolve
- `typmod_p`: Pointer to int32 where resolved type modifier will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeName](../L/LookupTypeName.md)
  - [TypeNameToString](../T/TypeNameToString.md)
- Called from (representative examples):
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)
  - [ATExecAddOf](../A/ATExecAddOf.md)
  - [DefineType](../D/DefineType.md)
  - [DefineDomain](../D/DefineDomain.md)
  - [AlterType](../A/AlterType.md)
  - [typenameTypeId](typenameTypeId.md)
  - [typenameTypeIdAndMod](typenameTypeIdAndMod.md)
  - [transformColumnDefinition](transformColumnDefinition.md)
  - [transformOfType](transformOfType.md)
  - [transformColumnType](transformColumnType.md)

## Notes and Other Information
Located in src/backend/parser/parse_type.c:264-290. This function is the preferred choice for most code that needs to resolve type names, as it provides complete validation and error handling. Unlike the lower-level LookupTypeName functions, callers can safely assume the returned Type represents a fully valid type without additional checking. The function always raises errors rather than returning NULL, simplifying error handling for callers.