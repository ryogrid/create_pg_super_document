# CreateStmt

## Location
[src/include/nodes/parsenodes.h:2648-2664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2648-L2664)

## Overview
CreateStmt is a parse tree node structure that represents a CREATE TABLE statement, containing all the necessary information to define a new table including columns, constraints, inheritance, partitioning, and storage options.

## Definition
```c
typedef struct CreateStmt
{
    NodeTag         type;
    RangeVar       *relation;        /* relation to create */
    List           *tableElts;       /* column definitions (list of ColumnDef) */
    List           *inhRelations;    /* relations to inherit from (list of RangeVar) */
    PartitionBoundSpec *partbound;   /* FOR VALUES clause */
    PartitionSpec  *partspec;        /* PARTITION BY clause */
    TypeName       *ofTypename;      /* OF typename */
    List           *constraints;     /* constraints (list of Constraint nodes) */
    List           *options;         /* options from WITH clause */
    OnCommitAction  oncommit;        /* what do we do at COMMIT? */
    char           *tablespacename;  /* table space to use, or NULL */
    char           *accessMethod;    /* table access method */
    bool            if_not_exists;   /* just do nothing if it already exists? */
} CreateStmt;
```

## Detailed Description
CreateStmt represents the parsed structure of a CREATE TABLE command in PostgreSQL. This comprehensive structure captures all aspects of table creation including column definitions, constraints, inheritance relationships, partitioning specifications, and storage options.

During parsing, the raw output initially intermixes ColumnDef and Constraint nodes in tableElts with constraints being NIL. After parse analysis, the structure is reorganized so that tableElts contains only ColumnDef nodes while constraints contains only Constraint nodes (primarily CONSTR_CHECK nodes in the current implementation).

The structure supports PostgreSQL's advanced table features including table inheritance, declarative partitioning, typed tables (created from composite types), and various storage options.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreateStmt node type
- `relation`: RangeVar specifying the name and schema of the table to create
- `tableElts`: List of ColumnDef nodes defining the table columns
- `inhRelations`: List of RangeVar nodes specifying parent tables for inheritance
- `partbound`: PartitionBoundSpec defining the FOR VALUES clause for partition tables
- `partspec`: PartitionSpec defining the PARTITION BY clause for partitioned tables
- `ofTypename`: TypeName for creating typed tables (CREATE TABLE ... OF type_name)
- `constraints`: List of Constraint nodes (primarily CHECK constraints after parse analysis)
- `options`: List of storage options from the WITH clause
- `oncommit`: OnCommitAction specifying behavior for temporary tables at commit time
- `tablespacename`: String specifying the tablespace name, or NULL for default
- `accessMethod`: String specifying the table access method (heap, etc.)
- `if_not_exists`: Boolean flag for CREATE TABLE IF NOT EXISTS behavior

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](../R/RangeVar.md) (for relation and inheritance specifications)
  - [PartitionBoundSpec](../P/PartitionBoundSpec.md) (for partition boundary definitions)
  - [PartitionSpec](../P/PartitionSpec.md) (for partitioning specifications)
  - [TypeName](../T/TypeName.md) (for typed table definitions)
  - OnCommitAction (for temporary table commit behavior)

- Called from (representative examples):
  - [create_ctas_internal](../c/create_ctas_internal.md) (src/backend/commands/createas.c:82)
  - [DefineSequence](../D/DefineSequence.md) (src/backend/commands/sequence.c:127)
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:698)
  - [DefineCompositeType](../D/DefineCompositeType.md) (src/backend/commands/typecmds.c:2520)
  - [transformCreateStmt](../t/transformCreateStmt.md) (src/backend/parser/parse_utilcmd.c:163)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1140)

## Notes and Other Information
- This structure undergoes significant transformation during parse analysis, with constraints being separated from column definitions
- Supports advanced PostgreSQL features including inheritance, partitioning, typed tables, and custom access methods
- The structure is used for regular tables, temporary tables, unlogged tables, and foreign tables (through inheritance in CreateForeignTableStmt)
- [Constraint](Constraint.md) processing follows a two-phase approach: initial parsing intermixes constraints with column definitions, followed by reorganization during parse analysis
- Access method specification allows for pluggable storage engines beyond the default heap storage