# RI_ConstraintInfo

## Location
[src/backend/utils/adt/ri_triggers.c:100-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L100-L125)

## Overview
RI_ConstraintInfo is a structure that stores information extracted from a foreign key constraint entry in pg_constraint and is cached in ri_constraint_cache for efficient access during referential integrity operations.

## Definition

```c
typedef struct RI_ConstraintInfo
{
	Oid			constraint_id;	/* OID of pg_constraint entry (hash key) */
	bool		valid;			/* successfully initialized? */
	Oid			constraint_root_id; /* OID of topmost ancestor constraint;
									 * same as constraint_id if not inherited */
	uint32		oidHashValue;	/* hash value of constraint_id */
	uint32		rootHashValue;	/* hash value of constraint_root_id */
	NameData	conname;		/* name of the FK constraint */
	Oid			pk_relid;		/* referenced relation */
	Oid			fk_relid;		/* referencing relation */
	char		confupdtype;	/* foreign key's ON UPDATE action */
	char		confdeltype;	/* foreign key's ON DELETE action */
	int			ndelsetcols;	/* number of columns referenced in ON DELETE
								 * SET clause */
	int16		confdelsetcols[RI_MAX_NUMKEYS]; /* attnums of cols to set on
												 * delete */
	char		confmatchtype;	/* foreign key's match type */
	int			nkeys;			/* number of key columns */
	int16		pk_attnums[RI_MAX_NUMKEYS]; /* attnums of referenced cols */
	int16		fk_attnums[RI_MAX_NUMKEYS]; /* attnums of referencing cols */
	Oid			pf_eq_oprs[RI_MAX_NUMKEYS]; /* equality operators (PK = FK) */
	Oid			pp_eq_oprs[RI_MAX_NUMKEYS]; /* equality operators (PK = PK) */
	Oid			ff_eq_oprs[RI_MAX_NUMKEYS]; /* equality operators (FK = FK) */
	dlist_node	valid_link;		/* Link in list of valid entries */
} RI_ConstraintInfo;
```
## Detailed Description
RI_ConstraintInfo serves as a cached representation of foreign key constraint metadata to optimize referential integrity checking operations. This structure contains all necessary information about a foreign key relationship, including the participating tables, column mappings, constraint actions, and operator information needed for equality comparisons. The structure supports constraint inheritance through the constraint_root_id field and maintains hash values for efficient cache lookups.

## Parameters / Member Variables
- `constraint_id`: OID of the pg_constraint entry, used as the hash key for cache lookup
- `valid`: Boolean flag indicating whether the constraint info was successfully initialized
- `constraint_root_id`: OID of the topmost ancestor constraint in inheritance hierarchies
- `oidHashValue`: Pre-computed hash value of constraint_id for cache efficiency
- `rootHashValue`: Pre-computed hash value of constraint_root_id for cache efficiency
- `conname`: Name of the foreign key constraint
- `pk_relid`: OID of the referenced (primary key) relation
- `fk_relid`: OID of the referencing (foreign key) relation
- `confupdtype`: Character code for the ON UPDATE action (e.g., 'a' for NO ACTION, 'c' for CASCADE)
- `confdeltype`: Character code for the ON DELETE action (e.g., 'a' for NO ACTION, 'c' for CASCADE)
- `ndelsetcols`: Number of columns referenced in ON DELETE SET clause
- `confdelsetcols[RI_MAX_NUMKEYS]`: Array of attribute numbers for columns to set on delete
- `confmatchtype`: Foreign key match type (e.g., 'f' for FULL, 'p' for PARTIAL, 's' for SIMPLE)
- `nkeys`: Number of key columns in the constraint
- `pk_attnums[RI_MAX_NUMKEYS]`: Array of attribute numbers for referenced columns
- `fk_attnums[RI_MAX_NUMKEYS]`: Array of attribute numbers for referencing columns
- `pf_eq_oprs[RI_MAX_NUMKEYS]`: Array of equality operators for primary key to foreign key comparisons
- `pp_eq_oprs[RI_MAX_NUMKEYS]`: Array of equality operators for primary key to primary key comparisons
- `ff_eq_oprs[RI_MAX_NUMKEYS]`: Array of equality operators for foreign key to foreign key comparisons
- `valid_link`: Linked list node for maintaining list of valid cache entries
## Dependencies
- Functions called/Symbols referenced:
  - [NameData](../N/NameData.md)
  - RI_MAX_NUMKEYS
  - [dlist_node](../d/dlist_node.md)
- Called from (representative examples):
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md)
  - [ri_LoadConstraintInfo](../r/ri_LoadConstraintInfo.md)
  - [ri_PerformCheck](../r/ri_PerformCheck.md)
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md)
  - [RI_FKey_cascade_del](RI_FKey_cascade_del.md)
  - [RI_FKey_cascade_upd](RI_FKey_cascade_upd.md)

## Notes and Other Information
This structure is central to PostgreSQL's referential integrity system and is heavily used during trigger execution for foreign key constraint checking. The caching mechanism improves performance by avoiding repeated lookups of constraint metadata from the system catalogs. The structure supports both simple and complex foreign key relationships, including those with multiple columns and various constraint actions.