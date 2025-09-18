# typenameTypeId

## Location
[src/backend/parser/parse_type.c:291-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L291-L309)

## Overview
typenameTypeId provides a safe, validated interface for obtaining a PostgreSQL type OID from a TypeName, ensuring the type is fully defined and valid.

## Definition
```c
Oid typenameTypeId(ParseState *pstate, const TypeName *typeName)
```

## Detailed Description
typenameTypeId combines the validation guarantees of typenameType with the convenience of returning only the type OID. This function is the recommended high-level interface when only the type OID is needed, as it ensures the resolved type is fully defined (not a shell type) and properly handles all error conditions. The function is widely used throughout PostgreSQL's command processing, particularly in DDL operations where type OIDs are needed for catalog updates, function definitions, and constraint processing.

## Parameters / Member Variables
- `pstate`: ParseState pointer for error location reporting and context
- `typeName`: TypeName structure containing the type specification to resolve

## Dependencies
- Functions called/Symbols referenced:
  - [typenameType](typenameType.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [objectNamesToOids](../o/objectNamesToOids.md)
  - [check_object_ownership](../c/check_object_ownership.md)
  - [DefineAggregate](../D/DefineAggregate.md)
  - [CreateFunction](../C/CreateFunction.md)
  - [CreateCast](../C/CreateCast.md)
  - [CreateTransform](../C/CreateTransform.md)
  - [DefineOpClass](../D/DefineOpClass.md)
  - [DefineOperator](../D/DefineOperator.md)
  - [PrepareQuery](../P/PrepareQuery.md)
  - [DefineRelation](../D/DefineRelation.md)
  - [DefineType](../D/DefineType.md)
  - [AlterEnum](../A/AlterEnum.md)
  - [DefineRange](../D/DefineRange.md)
  - [AlterDomainDefault](../A/AlterDomainDefault.md)

## Notes and Other Information
Located in src/backend/parser/parse_type.c:291-309. This function is the preferred choice when only the type OID is needed from a type name, as it provides the same validation as typenameType but with simpler return handling. Unlike LookupTypeNameOid, this function guarantees the type is fully defined and not just a shell type. The function properly manages system cache resources and is extensively used throughout PostgreSQL's DDL command processing.