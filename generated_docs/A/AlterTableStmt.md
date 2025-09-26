# AlterTableStmt

## Location
[src/include/nodes/parsenodes.h:2339-2346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2339-L2346)

## Overview
AlterTableStmt represents an ALTER TABLE statement that can contain multiple subcommands to modify table structure, constraints, indexes, and other properties.

## Definition
```c
typedef struct AlterTableStmt
{
    NodeTag     type;
    RangeVar   *relation;     /* table to work on */
    List       *cmds;         /* list of subcommands */
    ObjectType  objtype;      /* type of object */
    bool        missing_ok;   /* skip error if table missing */
} AlterTableStmt;
```

## Detailed Description
AlterTableStmt represents the SQL ALTER TABLE command, which can perform multiple schema modifications in a single statement. The statement supports a wide variety of operations including adding/dropping columns, modifying constraints, changing table ownership, and altering storage parameters.

The execution follows a three-phase approach: (1) examine subcommands and perform pre-transformation checking, (2) validate and transform subcommands while updating system catalogs, and (3) scan and optionally rewrite table data when required. This design allows multiple independent schema changes to be performed with only one pass over the data when table rewriting is necessary.

The objtype field specifies the type of database object being altered (table, index, sequence, view, etc.), allowing the same parse structure to handle ALTER commands for different object types. The missing_ok flag provides graceful handling of non-existent tables when IF EXISTS is specified.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AlterTableStmt node
- `relation`: RangeVar identifying the table/relation to be altered
- `cmds`: List of AlterTableCmd nodes representing the subcommands to execute
- `objtype`: ObjectType enum indicating what kind of object is being altered (table, index, etc.)
- `missing_ok`: Boolean flag indicating whether to skip execution if the target object doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar (for table identification)
  - ObjectType (for object type specification)
  - NodeTag (inherited node type system)
  - List (for subcommand storage)
- Called from (representative examples):
  - AlterTable (main execution function)
  - ATController (alter table controller)
  - ProcessUtilitySlow (utility command processing)
  - transformAlterTableStmt (statement transformation)

## Notes and Other Information
- Supports complex multi-step table alterations in a single statement
- Three-phase execution model optimizes table rewriting operations
- Can operate on tables, indexes, sequences, views, and other relation types
- Subcommands are executed in dependency order to avoid conflicts
- Table locking level is determined by the most restrictive subcommand
- Inheritance hierarchies are processed recursively when appropriate
- MVCC ensures atomicity - any error rolls back the entire operation
- Some operations require table rewriting while others only modify catalogs