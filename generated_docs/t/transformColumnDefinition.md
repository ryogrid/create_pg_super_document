# transformColumnDefinition

## Location
src/backend/parser/parse_utilcmd.c: 562 - 902

## Overview
Transforms a single ColumnDef within CREATE TABLE or ALTER TABLE ADD COLUMN statements, processing column types, constraints, and special column types like SERIAL and IDENTITY.

## Definition


## Detailed Description
transformColumnDefinition processes individual column definitions during table creation or alteration. It handles the complete transformation of column specifications including:

1. **SERIAL pseudo-type processing**: Converts SERIAL, BIGSERIAL, SMALLSERIAL types into their underlying integer types and generates associated sequence infrastructure
2. **Column type transformation**: Processes the column's data type specification through transformColumnType
3. **Constraint processing**: Validates and categorizes various column constraints (NOT NULL, DEFAULT, CHECK, PRIMARY KEY, UNIQUE, FOREIGN KEY, IDENTITY, GENERATED)
4. **Conflict detection**: Identifies conflicting constraint specifications (e.g., multiple defaults, conflicting NULL/NOT NULL declarations)
5. **Foreign data wrapper options**: Handles per-column options for foreign tables

For SERIAL columns, the function automatically creates NOT NULL and DEFAULT nextval() constraints. For IDENTITY columns, it generates the underlying sequence and sets up proper ownership relationships. The function also enforces business rules about which constraint combinations are valid and which are mutually exclusive.

## Parameters / Member Variables
- : CreateStmtContext containing parsing state and accumulating lists of various statement types
- : ColumnDef structure representing the column being processed

## Dependencies
- Functions called/Symbols referenced:
  - transformColumnType
  - generateSerialExtraStmts
  - transformConstraintAttrs
  - quote_qualified_identifier
  - typenameType
  - makeFuncCall
  - makeNode (A_Const, TypeCast, Constraint, AlterTableStmt, AlterTableCmd)
  - makeString
  - SystemTypeName
  - SystemFuncName
- Called from (representative examples):
  - transformCreateStmt
  - transformAlterTableStmt

## Notes and Other Information
The function maintains strict validation of constraint combinations, preventing conflicting specifications like both DEFAULT and IDENTITY on the same column. SERIAL types are treated as pseudo-types that expand into integer types with associated sequences and constraints. For identity columns, the function ensures they are implicitly NOT NULL and validates that they're not used on typed tables or partitions. Foreign table columns have restricted constraint support, prohibiting PRIMARY KEY, UNIQUE, and FOREIGN KEY constraints. The function accumulates constraints into different lists within the CreateStmtContext for later processing in the appropriate order.