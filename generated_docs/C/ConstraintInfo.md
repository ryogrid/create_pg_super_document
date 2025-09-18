# ConstraintInfo

## Location
src/bin/pg_dump/pg_dump.h: 494 - 495

## Overview
ConstraintInfo is a structure used by pg_dump to store metadata about database constraints (CHECK, FOREIGN KEY, UNIQUE, PRIMARY KEY, etc.) during the dump and restore process.

## Definition
```c
typedef struct _constraintInfo
{
    DumpableObject dobj;
    TableInfo  *contable;       /* NULL if domain constraint */
    TypeInfo   *condomain;      /* NULL if table constraint */
    char        contype;
    char       *condef;         /* definition, if CHECK or FOREIGN KEY */
    Oid         confrelid;      /* referenced table, if FOREIGN KEY */
    DumpId      conindex;       /* identifies associated index if any */
    bool        condeferrable;  /* true if constraint is DEFERRABLE */
    bool        condeferred;    /* true if constraint is INITIALLY DEFERRED */
    bool        conislocal;     /* true if constraint has local definition */
    bool        separate;       /* true if must dump as separate item */
} ConstraintInfo;
```

## Detailed Description
ConstraintInfo represents constraint metadata in PostgreSQL's pg_dump utility. It extends the base DumpableObject structure to include constraint-specific information necessary for dumping and restoring various types of constraints. The structure handles both table constraints and domain constraints, storing different constraint types including CHECK, FOREIGN KEY, UNIQUE, and PRIMARY KEY constraints with their associated properties and dependencies.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common dump metadata (OID, name, etc.)
- `contable`: Pointer to the TableInfo structure if this is a table constraint; NULL for domain constraints
- `condomain`: Pointer to the TypeInfo structure if this is a domain constraint; NULL for table constraints  
- `contype`: Character indicating the constraint type (corresponding to pg_constraint.contype)
- `condef`: String containing the constraint definition for CHECK or FOREIGN KEY constraints
- `confrelid`: OID of the referenced table for FOREIGN KEY constraints
- `conindex`: DumpId identifying any associated index (for UNIQUE/PRIMARY KEY constraints)
- `condeferrable`: Boolean flag indicating if the constraint can be deferred
- `condeferred`: Boolean flag indicating if the constraint is initially deferred
- `conislocal`: Boolean flag indicating if the constraint has a local definition (inheritance-related)
- `separate`: Boolean flag indicating if the constraint must be dumped as a separate item

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - [TableInfo](../T/TableInfo.md) (for table association)
  - [TypeInfo](../T/TypeInfo.md) (for domain association)
- Called from (representative examples):
  - [getTableDataFKConstraints](../g/getTableDataFKConstraints.md) (src/bin/pg_dump/pg_dump.c:3026)
  - [getConstraints](../g/getConstraints.md) (src/bin/pg_dump/pg_dump.c:7830, 7899)
  - [getDomainConstraints](../g/getDomainConstraints.md) (src/bin/pg_dump/pg_dump.c:8012, 8060, 8068)
  - [dumpConstraint](../d/dumpConstraint.md) (src/bin/pg_dump/pg_dump.c:17237)
  - [repairTableConstraintMultiLoop](../r/repairTableConstraintMultiLoop.md) (src/bin/pg_dump/pg_dump_sort.c:1079)

## Notes and Other Information
- This structure handles all constraint types using a unified interface, with type-specific fields used as appropriate
- The condeferrable and condeferred fields are primarily valid for unique/primary-key constraints
- Foreign key constraints use a different objType for easier sorting during dump operations
- The separate flag determines whether the constraint needs to be dumped independently or can be included with its parent object
- Used extensively in dependency resolution and loop repair algorithms for proper dump ordering