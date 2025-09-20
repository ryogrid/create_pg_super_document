# ExtensionInfo

## Location
[src/bin/pg_dump/pg_dump.h:195-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L195-L196)

## Overview
ExtensionInfo represents a PostgreSQL extension object in pg_dump, storing metadata about installed extensions including their configuration, version, and namespace information.

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
ExtensionInfo extends DumpableObject to represent PostgreSQL extensions during the dump and restore process. Extensions are collections of SQL objects (functions, data types, operators, etc.) that are installed and managed as a unit. This structure contains all the metadata necessary to recreate an extension in the target database, including version information, relocatability, and configuration table details.

The structure is populated by the getExtensions() function, which queries the pg_extension system catalog joined with pg_namespace to retrieve all installed extensions. The information is used to generate CREATE EXTENSION statements during dump output and to track extension membership of other database objects.

## Parameters / Member Variables
- : Base DumpableObject containing metadata like catalog ID, dump ID, name, and dump components
- : String name of the schema (namespace) containing the extension's objects, obtained from pg_namespace.nspname
- : Boolean flag indicating whether the extension can be moved to a different schema after installation (from pg_extension.extrelocatable)
- : String version of the installed extension (from pg_extension.extversion)
- : String containing information about configuration tables associated with the extension (from pg_extension.extconfig)
- : String containing conditions for configuration tables (from pg_extension.extcondition)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
- Called from (representative examples):
  - [getExtensions](../g/getExtensions.md) (creates and populates ExtensionInfo arrays)
  - [findExtensionByOid](../f/findExtensionByOid.md) (searches for ExtensionInfo by OID)
  - [selectDumpableExtension](../s/selectDumpableExtension.md) (determines if extension should be dumped)
  - [dumpExtension](../d/dumpExtension.md) (outputs CREATE EXTENSION statements)
  - [checkExtensionMembership](../c/checkExtensionMembership.md) (checks if objects belong to extensions)
  - [recordExtensionMembership](../r/recordExtensionMembership.md) (records object-extension relationships)
  - [getExtensionMembership](../g/getExtensionMembership.md) (retrieves extension membership data)

## Notes and Other Information
- Unlike many other dumpable objects, ExtensionInfo does not include DumpableAcl since extensions themselves do not have ACLs
- The extconfig and extcondition fields work together to handle configuration tables that should be dumped with their data rather than just their schema
- Extensions can be relocatable (movable between schemas) or non-relocatable (fixed to a specific schema)
- Extension membership tracking is crucial for determining which objects should be skipped during dump (since they're managed by the extension)
- Used by pg_dump to generate CREATE EXTENSION statements and manage extension dependencies
- Located in src/bin/pg_dump/pg_dump.h:187-195