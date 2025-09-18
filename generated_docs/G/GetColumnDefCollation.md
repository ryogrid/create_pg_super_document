# GetColumnDefCollation

## Location
[src/backend/parser/parse_type.c:540-577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L540-L577)

## Overview
GetColumnDefCollation determines the appropriate collation to be used for a column being defined, considering the column definition and the column's data type.

## Definition
```c
Oid GetColumnDefCollation(ParseState *pstate, const ColumnDef *coldef, Oid typeOid)
```

## Detailed Description
This function implements the logic for determining what collation should be applied to a column during table creation or modification. It follows a priority hierarchy: explicit COLLATE clauses take precedence, followed by precooked collation specifications, and finally falling back to the data type's default collation.

The function also performs validation to ensure that collations are only applied to collatable data types, raising an error if a collation is specified for a type that doesn't support collations.

## Parameters / Member Variables
- `pstate`: ParseState pointer for error location tracking; can be NULL if error position tracking is not needed
- `coldef`: ColumnDef structure containing the column definition including any collation specifications
- `typeOid`: OID of the column's data type, used to determine type-specific collation behavior

## Dependencies
- Functions called/Symbols referenced:
  - [ColumnDef](../C/ColumnDef.md)
  - [get_typcollation](../g/get_typcollation.md)
  - [LookupCollation](../L/LookupCollation.md)
  - OidIsValid
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [format_type_be](../f/format_type_be.md)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - [BuildDescForRelation](../B/BuildDescForRelation.md)
  - [MergeChildAttribute](../M/MergeChildAttribute.md)
  - [MergeInheritedAttribute](../M/MergeInheritedAttribute.md)
  - [ATExecAddColumn](../A/ATExecAddColumn.md)
  - [ATPrepAlterColumnType](../A/ATPrepAlterColumnType.md)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)

## Notes and Other Information
- Implements a three-tier priority system for collation determination: explicit COLLATE clause > precooked collOid > type default
- Validates that collations are only applied to collatable types, preventing runtime errors
- Used extensively in DDL operations involving column creation and modification
- Returns InvalidOid for non-collatable types when no explicit collation is specified
- Error reporting includes precise location information when ParseState is available
- Located in src/backend/parser/parse_type.c:540-577