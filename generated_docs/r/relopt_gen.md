# relopt_gen

## Location
[src/include/access/reloptions.h:64-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/reloptions.h#L64-L73)

## Overview
relopt_gen is the base structure that holds shared data common to all relation option types in PostgreSQL's reloption system, serving as the foundation for type-specific option structures.

## Definition

```c
typedef struct relopt_gen
{
	const char *name;			/* must be first (used as list termination
								 * marker) */
	const char *desc;
	bits32		kinds;
	LOCKMODE	lockmode;
	int			namelen;
	relopt_type type;
} relopt_gen;
```
## Detailed Description
The relopt_gen structure serves as the generic base for all relation option definitions in PostgreSQL. It contains the common metadata that every relation option must have, regardless of its specific data type (bool, int, real, enum, or string). This structure is embedded as the first member in all type-specific relation option structures (relopt_bool, relopt_int, relopt_real, etc.), allowing them to be treated polymorphically through the common relopt_gen interface.

The structure is designed with the name field first specifically to serve as a list termination marker when processing arrays of relation options. The kinds field uses bitmasks to specify which types of database objects (heap tables, indexes, etc.) can use this particular option.

## Parameters / Member Variables
- `*name`: The name of the relation option as it appears in SQL statements; must be the first field as it's used as a null-termination marker for option arrays
- `*desc`: A human-readable description of what this option does
- `kinds`: A bitmask of relopt_kind values indicating which database object types (heap, toast, btree, etc.) can use this option
- `lockmode`: The lock level required on the relation when this option is modified
- `namelen`: The length of the name string for optimization purposes
- `type`: The data type of this option (RELOPT_TYPE_BOOL, RELOPT_TYPE_INT, RELOPT_TYPE_REAL, RELOPT_TYPE_ENUM, or RELOPT_TYPE_STRING)
## Dependencies
- Functions called/Symbols referenced:
  - bits32
  - relopt_type
- Called from (representative examples):
  - [add_reloption](../a/add_reloption.md)
  - [add_bool_reloption](../a/add_bool_reloption.md)
  - [add_int_reloption](../a/add_int_reloption.md)
  - [add_real_reloption](../a/add_real_reloption.md)
  - [allocate_reloption](../a/allocate_reloption.md)

## Notes and Other Information
This structure is the cornerstone of PostgreSQL's relation options system, enabling type-safe handling of diverse option types while maintaining a unified interface. All specific option types (relopt_bool, relopt_int, etc.) embed this structure as their first member, allowing for polymorphic access through casting. The name field's position is critical for the option parsing machinery that relies on null-terminated arrays.