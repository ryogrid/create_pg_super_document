# ConstraintCategory

## Location
[src/include/catalog/pg_constraint.h:208-278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_constraint.h#L208-L278)

## Overview
ConstraintCategory is an enumeration type that categorizes different types of constraints in PostgreSQL for lookup and identification purposes.

## Definition

```c
structFkConstraintRow(HeapTuple tuple, int *numfks,
									   AttrNumber *conkey, AttrNumber *confkey,
									   Oid *pf_eq_oprs, Oid *pp_eq_oprs, Oid *ff_eq_oprs,
									   int *num_fk_del_set_cols, AttrNumber *fk_del_set_cols);
```
## Detailed Description
ConstraintCategory is a classification enum used internally by PostgreSQL to distinguish between different categories of constraints when performing constraint-related operations. This enumeration helps the system determine how to handle constraint lookups, validation, and management operations based on the context in which the constraint exists.

The enum is primarily used in constraint management functions to specify whether operations should target relation-level constraints (such as primary keys, foreign keys, check constraints on tables) or domain-level constraints (constraints applied to custom data types). The CONSTRAINT_ASSERTION value is reserved for future use, indicating PostgreSQL's planned support for SQL assertion constraints.

This categorization is essential for functions like ConstraintNameIsUsed(), which need to search for existing constraint names within the appropriate scope (either on relations or on domains) to avoid naming conflicts.

## Parameters / Member Variables
- : Indicates constraints that apply to relations (tables, views). This includes primary key, foreign key, unique, check, and exclusion constraints defined on tables
- : Indicates constraints that apply to domain types. Domain constraints are check constraints defined on custom data types created with CREATE DOMAIN
- : Reserved for future SQL assertion constraints. Currently unused but included for forward compatibility

## Dependencies
- Functions called/Symbols referenced:
  - No direct references (enum definition)
- Called from (representative examples):
  - [ConstraintNameIsUsed](ConstraintNameIsUsed.md) function in pg_constraint.c
  - Various constraint creation and management functions throughout the codebase
  - Index creation functions in index.c
  - Type command functions in typecmds.c
  - Table command functions in tablecmds.c

## Notes and Other Information
- The CONSTRAINT_ASSERTION value is included for future expansion but is not currently implemented in PostgreSQL
- This enum is used primarily for internal constraint management and is not directly exposed to SQL users
- The categorization helps optimize constraint lookups by limiting searches to the appropriate catalog entries
- Each category corresponds to different columns in the pg_constraint system catalog (conrelid for relations, contypid for domains)
- The enum values are used as parameters to functions that need to distinguish between different constraint contexts