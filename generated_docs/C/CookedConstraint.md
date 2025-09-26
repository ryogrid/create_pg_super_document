# CookedConstraint

## Location
src/include/catalog/heap.h: 35 - 47

## Overview
CookedConstraint is a structure that represents processed (cooked) constraint definitions for DEFAULT and CHECK constraints in PostgreSQL table creation and modification operations.

## Definition
```c
typedef struct CookedConstraint
{
    ConstrType  contype;        /* CONSTR_DEFAULT or CONSTR_CHECK */
    Oid         conoid;         /* constr OID if created, otherwise Invalid */
    char       *name;           /* name, or NULL if none */
    AttrNumber  attnum;         /* which attr (only for DEFAULT) */
    Node       *expr;           /* transformed default or check expr */
    bool        skip_validation; /* skip validation? (only for CHECK) */
    bool        is_local;       /* constraint has local (non-inherited) def */
    int         inhcount;       /* number of times constraint is inherited */
    bool        is_no_inherit;  /* constraint has local def and cannot be
                                 * inherited */
} CookedConstraint;
```

## Detailed Description
CookedConstraint represents constraint definitions that have been processed ("cooked") into their executable form, as opposed to "raw" constraint expressions that are still in parse tree format. This structure is primarily used during table creation and ALTER TABLE operations to handle constraint inheritance, storage, and validation.

The structure supports two main constraint types:
- **DEFAULT constraints**: Define default values for table columns
- **CHECK constraints**: Define validation rules that must be satisfied by column values

CookedConstraint is typically used when constraints are inherited from existing relations or when constraints need to be stored in the system catalogs. The "cooked" nature means that the expressions have been transformed into executable expression trees rather than remaining as untransformed parse trees.

## Parameters / Member Variables
- `contype`: The type of constraint, limited to CONSTR_DEFAULT or CONSTR_CHECK
- `conoid`: The OID assigned to the constraint after creation in system catalogs; set to InvalidOid before creation
- `name`: Optional constraint name; can be NULL for unnamed constraints
- `attnum`: Column attribute number that the constraint applies to (used only for DEFAULT constraints)
- `expr`: The transformed constraint expression as an executable expression tree (Node)
- `skip_validation`: Flag to skip validation of existing table data when adding CHECK constraints
- `is_local`: Indicates whether the constraint has a local (non-inherited) definition
- `inhcount`: Counter tracking how many times this constraint has been inherited through table inheritance
- `is_no_inherit`: Flag indicating the constraint is local-only and cannot be inherited by child tables

## Dependencies
- Functions called/Symbols referenced:
  - ConstrType (enum type)
  - Oid (type)
  - AttrNumber (type) 
  - Node (type)

- Called from (representative examples):
  - StoreConstraints (src/backend/catalog/heap.c:2257)
  - AddRelationNewConstraints (src/backend/catalog/heap.c:2331, 2391, 2520)
  - DefineRelation (src/backend/commands/tablecmds.c:944, 946)
  - MergeCheckConstraint (src/backend/commands/tablecmds.c:3055, 3059, 3088)
  - ATAddCheckConstraint (src/backend/commands/tablecmds.c:9508)

## Notes and Other Information
- CookedConstraint is used specifically for pre-processed constraints, typically those inherited from existing relations
- The distinction between "cooked" and "raw" constraints is important: cooked constraints contain executable expression trees while raw constraints contain untransformed parse trees
- Only DEFAULT and CHECK constraint types are supported; other constraint types like PRIMARY KEY, FOREIGN KEY, etc. are handled differently
- The inheritance-related fields (is_local, inhcount, is_no_inherit) are crucial for PostgreSQL's table inheritance feature
- When constraints are stored via StoreConstraints(), the conoid field is updated with the actual OID assigned by the system catalogs