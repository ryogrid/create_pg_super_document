# _typeInfo

## Location
[src/bin/pg_dump/pg_dump.h:197-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L197-L222)

## Overview
The _typeInfo structure represents type information used by the PostgreSQL pg_dump utility to store metadata about database types during the dump process.

## Definition

```c
typedef struct _typeInfo
{
	DumpableObject dobj;
	DumpableAcl dacl;

	/*
	 * Note: dobj.name is the raw pg_type.typname entry.  ftypname is the
	 * result of format_type(), which will be quoted if needed, and might be
	 * schema-qualified too.
	 */
	char	   *ftypname;
	const char *rolname;
	Oid			typelem;
	Oid			typrelid;
	char		typrelkind;		/* 'r', 'v', 'c', etc */
	char		typtype;		/* 'b', 'c', etc */
	bool		isArray;		/* true if auto-generated array type */
	bool		isMultirange;	/* true if auto-generated multirange type */
	bool		isDefined;		/* true if typisdefined */
	/* If needed, we'll create a "shell type" entry for it; link that here: */
	struct _shellTypeInfo *shellType;	/* shell-type entry, or NULL */
	/* If it's a domain, its not-null constraint is here: */
	struct _constraintInfo *notnull;
	/* If it's a domain, we store links to its CHECK constraints here: */
	int			nDomChecks;
	struct _constraintInfo *domChecks;
} TypeInfo;
```
## Detailed Description
The _typeInfo structure is a comprehensive data structure used by pg_dump to manage type information during database dumping operations. It extends the base DumpableObject and DumpableAcl structures to provide type-specific metadata. This structure handles various PostgreSQL type categories including base types, composite types, domains, arrays, and multirange types. It maintains both raw type names and formatted type names, supports shell type references for forward declarations, and includes specialized fields for domain constraints.

## Parameters / Member Variables
- `dobj`: Base dumpable object structure containing common dump metadata
- `dacl`: Access control list information for the type
- `*ftypname`: Formatted type name (quoted and potentially schema-qualified)
- `*rolname`: Role/owner name of the type
- `typelem`: OID of the element type (for arrays and ranges)
- `typrelid`: OID of the relation associated with this type (for composite types)
- `typrelkind`: Relation kind character ('r' for table, 'v' for view, 'c' for composite, etc.)
- `typtype`: Type category character ('b' for base, 'c' for composite, 'd' for domain, etc.)
- `isArray`: Boolean flag indicating if this is an auto-generated array type
- `isMultirange`: Boolean flag indicating if this is an auto-generated multirange type
- `isDefined`: Boolean flag indicating if the type is fully defined (typisdefined)
- `*shellType`: Pointer to associated shell type entry, used for forward declarations
- `*notnull`: Pointer to not-null constraint information (for domain types)
- `nDomChecks`: Number of CHECK constraints for domain types
- `*domChecks`: Array of pointers to CHECK constraint information for domains
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
  - [_shellTypeInfo](../s/_shellTypeInfo.md)
  - [_constraintInfo](../c/_constraintInfo.md)
- Called from (representative examples):
  - No direct references found (likely used internally by pg_dump functions)

## Notes and Other Information
- This structure is central to pg_dump's type management system
- The distinction between dobj.name (raw typname) and ftypname (formatted) allows proper handling of schema-qualified and quoted type names
- Shell types are used to handle forward references in type dependencies
- Domain-specific fields (notnull, nDomChecks, domChecks) enable proper constraint dumping
- The structure supports PostgreSQL's rich type system including composite types, domains, arrays, and the newer multirange types