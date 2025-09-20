# CreateConstraintEntry

## Location
[src/backend/catalog/pg_constraint.c:48-398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L48-L398)

## Overview
Creates a constraint table entry in the pg_constraint catalog and establishes necessary dependency relationships, but does not create subsidiary records like triggers or indexes.

## Definition

```c
struct_array_builtin(conkey, constraintNKeys, INT2OID);
```
## Detailed Description
This function is the core mechanism for creating constraint entries in PostgreSQL's pg_constraint system catalog. It handles all types of constraints including CHECK, PRIMARY KEY, UNIQUE, FOREIGN KEY, EXCLUSION, and domain constraints. The function converts C arrays to PostgreSQL arrays, populates all catalog fields, and establishes both automatic and normal dependency relationships. It does not create subsidiary objects like indexes or triggers - those are handled separately by calling code.

## Parameters / Member Variables
- : Name of the constraint to be created
- : OID of the namespace containing the constraint
- : Single character indicating constraint type (c=CHECK, p=PRIMARY KEY, u=UNIQUE, f=FOREIGN KEY, x=EXCLUSION, t=trigger constraint, n=NOT NULL)
- : Whether the constraint can be deferred
- : Whether the constraint is initially deferred
- : Whether the constraint has been validated
- : OID of parent constraint (for inherited constraints)
- : OID of the relation the constraint applies to
- : Array of column numbers that the constraint applies to
- : Number of key columns in constraintKey array
- : Total number of key columns including those inherited
- : OID of domain for domain constraints
- : OID of supporting index for UNIQUE/PRIMARY KEY/EXCLUSION constraints
- : OID of referenced relation for FOREIGN KEY constraints
- : Array of column numbers in the foreign key
- : Array of PK/FK equality operator OIDs
- : Array of PK/PK equality operator OIDs
- : Array of FK/FK equality operator OIDs
- : Number of foreign key columns
- : Foreign key UPDATE action (r=RESTRICT, c=CASCADE, n=SET NULL, d=SET DEFAULT, a=NO ACTION)
- : Foreign key DELETE action (same codes as UPDATE)
- : Columns to set for SET NULL/DEFAULT on delete
- : Number of columns in fkDeleteSetCols
- : Foreign key match type (f=FULL, p=PARTIAL, s=SIMPLE)
- : Array of exclusion operator OIDs for EXCLUSION constraints
- : Parsed CHECK constraint expression
- : Binary representation of CHECK constraint expression
- : Whether constraint is locally defined (not inherited)
- : Number of direct inheritance ancestors that also have this constraint
- : Whether constraint should not be inherited by child tables
- : Whether this is an internal system-generated constraint

## Dependencies
- Functions called/Symbols referenced:
  - namestrcpy
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [construct_array_builtin](../c/construct_array_builtin.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - ObjectAddressSet
  - [new_object_addresses](../n/new_object_addresses.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md)
  - InvokeObjectPostCreateHookArg
- Called from (representative examples):
  - [StoreRelCheck](../S/StoreRelCheck.md)
  - [index_constraint_create](../i/index_constraint_create.md)
  - [addFkConstraint](../a/addFkConstraint.md)
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md)
  - [domainAddNotNullConstraint](../d/domainAddNotNullConstraint.md)

## Notes and Other Information
- Returns the OID of the newly created constraint entry
- Handles conversion of C arrays to PostgreSQL array types for storage in the catalog
- Establishes AUTO dependencies to owning relations/domains and NORMAL dependencies to referenced objects
- Does not validate constraint data - validation is handled elsewhere
- Thread-safe as it uses proper catalog locking mechanisms