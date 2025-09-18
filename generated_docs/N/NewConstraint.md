# NewConstraint

## Location
[src/backend/commands/tablecmds.c:212-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L212-L221)

## Overview
NewConstraint is a structure used during PostgreSQL's ALTER TABLE operations to represent constraints that need to be validated during the Phase 3 table scan. It handles both CHECK and FOREIGN KEY constraints that are added to tables.

## Definition
```c
typedef struct NewConstraint
{
    char       *name;        /* Constraint name, or NULL if none */
    ConstrType  contype;     /* CHECK or FOREIGN */
    Oid         refrelid;    /* PK rel, if FOREIGN */
    Oid         refindid;    /* OID of PK's index, if FOREIGN */
    Oid         conid;       /* OID of pg_constraint entry, if FOREIGN */
    Node       *qual;        /* Check expr or CONSTR_FOREIGN Constraint */
    ExprState  *qualstate;   /* Execution state for CHECK expr */
} NewConstraint;
```

## Detailed Description
NewConstraint is used specifically during the Phase 3 table scan of ALTER TABLE operations to validate newly added constraints against existing table data. The structure is designed to handle both CHECK constraints (which validate expressions against each row) and FOREIGN KEY constraints (which validate referential integrity). For CHECK constraints, it stores the expression and its execution state, while for FOREIGN KEY constraints, it stores references to the primary key relation and associated metadata. Note that NOT NULL constraints are handled separately and do not use this structure.

## Parameters / Member Variables
- `name`: The name of the constraint, or NULL if the constraint is unnamed
- `contype`: The type of constraint (CHECK or FOREIGN from ConstrType enum)
- `refrelid`: For FOREIGN KEY constraints, the OID of the referenced primary key relation
- `refindid`: For FOREIGN KEY constraints, the OID of the primary key's index
- `conid`: For FOREIGN KEY constraints, the OID of the pg_constraint catalog entry
- `qual`: The constraint expression for CHECK constraints, or Constraint node for FOREIGN KEY
- `qualstate`: Execution state for CHECK constraint expressions (compiled for evaluation)

## Dependencies
- Functions called/Symbols referenced:
  - ConstrType (enum type)
  - Standard PostgreSQL types (Node, ExprState, Oid)
- Called from (representative examples):
  - [ATRewriteTables](../A/ATRewriteTables.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [ATAddCheckConstraint](../A/ATAddCheckConstraint.md)
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md)
  - [ATExecValidateConstraint](../A/ATExecValidateConstraint.md)

## Notes and Other Information
- This structure is specifically for Phase 3 constraint validation during table rewrites
- NOT NULL constraints are handled by a separate mechanism and do not use this structure
- The qualstate field is compiled during constraint preparation for efficient row-by-row evaluation
- Used primarily during ADD CONSTRAINT operations that require validation against existing data
- Part of PostgreSQL's multi-phase ALTER TABLE processing system