# GetColumnDefCollation

## Location
src/backend/parser/parse_type.c: 540 - 577

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
  - ColumnDef
  - get_typcollation
  - LookupCollation
  - OidIsValid
  - ereport
  - errcode
  - errmsg
  - format_type_be
  - parser_errposition
- Called from (representative examples):
  - BuildDescForRelation
  - MergeChildAttribute
  - MergeInheritedAttribute
  - ATExecAddColumn
  - ATPrepAlterColumnType
  - ATExecAlterColumnType
  - addRangeTableEntryForFunction

## Notes and Other Information
- Implements a three-tier priority system for collation determination: explicit COLLATE clause > precooked collOid > type default
- Validates that collations are only applied to collatable types, preventing runtime errors
- Used extensively in DDL operations involving column creation and modification
- Returns InvalidOid for non-collatable types when no explicit collation is specified
- Error reporting includes precise location information when ParseState is available
- Located in src/backend/parser/parse_type.c:540-577