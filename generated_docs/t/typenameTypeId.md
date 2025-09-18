# typenameTypeId

## Location
src/backend/parser/parse_type.c: 291 - 309

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
  - typenameType
  - ReleaseSysCache
- Called from (representative examples):
  - objectNamesToOids
  - check_object_ownership
  - DefineAggregate
  - CreateFunction
  - CreateCast
  - CreateTransform
  - DefineOpClass
  - DefineOperator
  - PrepareQuery
  - DefineRelation
  - DefineType
  - AlterEnum
  - DefineRange
  - AlterDomainDefault

## Notes and Other Information
Located in src/backend/parser/parse_type.c:291-309. This function is the preferred choice when only the type OID is needed from a type name, as it provides the same validation as typenameType but with simpler return handling. Unlike LookupTypeNameOid, this function guarantees the type is fully defined and not just a shell type. The function properly manages system cache resources and is extensively used throughout PostgreSQL's DDL command processing.