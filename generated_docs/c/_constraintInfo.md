# _constraintInfo

## Location
[src/bin/pg_dump/pg_dump.h:481-493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L481-L493)

## Overview
The  structure represents all types of database constraints in PostgreSQL's pg_dump utility, including CHECK, FOREIGN KEY, unique, and primary key constraints.

## Definition

```c
typedef struct _constraintInfo
{
	DumpableObject dobj;
	TableInfo  *contable;		/* NULL if domain constraint */
	TypeInfo   *condomain;		/* NULL if table constraint */
	char		contype;
	char	   *condef;			/* definition, if CHECK or FOREIGN KEY */
	Oid			confrelid;		/* referenced table, if FOREIGN KEY */
	DumpId		conindex;		/* identifies associated index if any */
	bool		condeferrable;	/* true if constraint is DEFERRABLE */
	bool		condeferred;	/* true if constraint is INITIALLY DEFERRED */
	bool		conislocal;		/* true if constraint has local definition */
	bool		separate;		/* true if must dump as separate item */
} ConstraintInfo;
```
## Detailed Description
The  structure is a comprehensive data structure used by pg_dump to represent all constraint types in PostgreSQL. It uses a different  for foreign key constraints to facilitate proper sorting during the dump process. The structure supports both table constraints and domain constraints through its dual-pointer design ( and ).

## Parameters / Member Variables
- : Base  containing common metadata for dump operations
- : Pointer to the table this constraint belongs to; NULL for domain constraints
- : Pointer to the domain this constraint belongs to; NULL for table constraints  
- : Character code identifying the constraint type (CHECK, FOREIGN KEY, etc.)
- : Text definition of the constraint, used for CHECK and FOREIGN KEY constraints
- : OID of the referenced table for FOREIGN KEY constraints
- : DumpId that identifies any associated index for the constraint
- : Boolean indicating if the constraint is DEFERRABLE (valid for unique/primary key)
- : Boolean indicating if the constraint is INITIALLY DEFERRED (valid for unique/primary key)
- : Boolean indicating if the constraint has a local definition

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - [TableInfo](../T/TableInfo.md)
  - [TypeInfo](../T/TypeInfo.md)
  - DumpId
- Called from (representative examples):
  - [_typeInfo](../t/_typeInfo.md) (domain constraints)
  - [_tableInfo](../t/_tableInfo.md) (table constraints)

## Notes and Other Information
- The  and  fields are currently only valid for unique/primary-key constraints
- For other constraint types, deferral information is stored in the  field
- The structure uses a mutually exclusive design where either  or  is set, but not both
- This unified approach allows pg_dump to handle all constraint types through a single data structure while maintaining type-specific behavior through the  field