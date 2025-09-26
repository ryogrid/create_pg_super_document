# ColumnDef

## Location
[src/include/nodes/parsenodes.h:723-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L723-L746)

## Overview
ColumnDef represents a column definition structure used in various CREATE statements, containing comprehensive column metadata including data type, constraints, default values, and storage properties.

## Definition
```c
typedef struct ColumnDef
{
    NodeTag     type;
    char       *colname;            /* name of column */
    TypeName   *typeName;           /* type of column */
    char       *compression;        /* compression method for column */
    int         inhcount;           /* number of times column is inherited */
    bool        is_local;           /* column has local (non-inherited) def'n */
    bool        is_not_null;        /* NOT NULL constraint specified? */
    bool        is_from_type;       /* column definition came from table type */
    char        storage;            /* attstorage setting, or 0 for default */
    char       *storage_name;       /* attstorage setting name or NULL for default */
    Node       *raw_default;        /* default value (untransformed parse tree) */
    Node       *cooked_default;     /* default value (transformed expr tree) */
    char        identity;           /* attidentity setting */
    RangeVar   *identitySequence;   /* to store identity sequence name for ALTER TABLE ... ADD COLUMN */
    char        generated;          /* attgenerated setting */
    CollateClause *collClause;      /* untransformed COLLATE spec, if any */
    Oid         collOid;            /* collation OID (InvalidOid if not set) */
    List       *constraints;        /* other constraints on column */
    List       *fdwoptions;         /* per-column FDW options */
    ParseLoc    location;           /* parse location, or -1 if none/unknown */
} ColumnDef;
```

## Detailed Description
ColumnDef is a comprehensive structure representing column definitions in CREATE TABLE, ALTER TABLE, and related DDL statements. It handles both raw parse tree forms and post-analysis forms of default values and collation specifications. The structure supports inheritance tracking, identity columns, generated columns, and various storage options. It's designed to handle the dual nature of column definitions that can come from parsing or inheritance from existing relations.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a ColumnDef node
- `colname`: Name of the column
- `typeName`: Data type specification (TypeName structure)
- `compression`: Compression method for the column (if specified)
- `inhcount`: Number of times this column is inherited from parent tables
- `is_local`: Whether the column has a local (non-inherited) definition
- `is_not_null`: Whether NOT NULL constraint is specified
- `is_from_type`: Whether the column definition came from a table type
- `storage`: Storage setting for the column (attstorage), or 0 for default
- `storage_name`: Name of the storage setting, or NULL for default
- `raw_default`: Default value in untransformed parse tree form
- `cooked_default`: Default value in post-parse-analysis, executable form
- `identity`: Identity column setting (attidentity)
- `identitySequence`: Identity sequence name for ALTER TABLE ADD COLUMN
- `generated`: Generated column setting (attgenerated)
- `collClause`: Untransformed COLLATE specification
- `collOid`: Collation OID (InvalidOid if not set)
- `constraints`: List of other constraints on the column
- `fdwoptions`: Per-column foreign data wrapper options
- `location`: Parse location for error reporting, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - [TypeName](../T/TypeName.md)
  - storage
  - [storage_name](../s/storage_name.md)
  - [RangeVar](../R/RangeVar.md)
  - [CollateClause](CollateClause.md)
  - ParseLoc
- Called from (representative examples):
  - [transformCreateStmt](../t/transformCreateStmt.md)
  - [ATExecAddColumn](../A/ATExecAddColumn.md)
  - [MergeAttributes](../M/MergeAttributes.md)
  - [transformColumnDefinition](../t/transformColumnDefinition.md)
  - [makeColumnDef](../m/makeColumnDef.md)

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:723-746
- Never contains both raw_default and cooked_default simultaneously
- CONSTR_DEFAULT items in constraints are moved to raw_default during transformation
- Supports inheritance tracking through inhcount and is_local flags
- Handles both identity and generated columns
- Used extensively in table creation, alteration, and inheritance operations
- Storage settings reference the related storage_name symbol mentioned in processed symbols