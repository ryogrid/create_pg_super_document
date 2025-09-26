# CompositeTypeStmt

## Location
[src/include/nodes/parsenodes.h:3685-3690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3685-L3690)

## Overview
CompositeTypeStmt represents a CREATE TYPE statement for defining composite (record) types in PostgreSQL's parse tree structure.

## Definition
```c
typedef struct CompositeTypeStmt
{
    NodeTag     type;
    RangeVar   *typevar;        /* the composite type to be created */
    List       *coldeflist;     /* list of ColumnDef nodes */
} CompositeTypeStmt;
```

## Detailed Description
CompositeTypeStmt is a parse tree node that represents CREATE TYPE statements used to define composite types (also known as record types or row types) in PostgreSQL. Composite types allow users to create custom data types that consist of multiple named fields, similar to C structs or Pascal records. This statement encapsulates the type name and the list of column definitions that make up the composite type.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CompositeTypeStmt node
- `typevar`: RangeVar pointer specifying the name and schema of the composite type to be created
- `coldeflist`: List of ColumnDef nodes defining the fields/columns of the composite type, including their names, data types, and constraints

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar
  - NodeTag
  - List (containing ColumnDef nodes)
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
- Composite types can be used as column types in tables, function parameters, and return types
- The coldeflist contains ColumnDef structures that define each field of the composite type
- Composite types support nested structures and can reference other user-defined types
- Once created, composite types can be used in table definitions and function signatures
- The type system treats composite types as first-class citizens, allowing them to be used anywhere regular types can be used