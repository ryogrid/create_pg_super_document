# TypeInfo

## Location
[src/bin/pg_dump/pg_dump.h:197-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L197-L223)

## Overview
TypeInfo represents a PostgreSQL data type object in pg_dump, storing comprehensive metadata about types including their definition, ownership, element relationships, and associated constraints.

## Definition

```c
typedef struct _typeInfo
{
	DumpableObject dobj;
	DumpableAcl dacl;

	char	   *ftypname;
	const char *rolname;
	Oid			typelem;
	Oid			typrelid;
	char		typrelkind;		/* 'r', 'v', 'c', etc */
	char		typtype;		/* 'b', 'c', etc */
	bool		isArray;		/* true if auto-generated array type */
	bool		isMultirange;	/* true if auto-generated multirange type */
	bool		isDefined;		/* true if typisdefined */
	struct _shellTypeInfo *shellType;	/* shell-type entry, or NULL */
	struct _constraintInfo *notnull;
	int			nDomChecks;
	struct _constraintInfo *domChecks;
} TypeInfo;
```
## Detailed Description
TypeInfo extends DumpableObjectWithAcl to represent PostgreSQL data types during the dump and restore process. This includes all types: built-in types, user-defined types, domains, composite types, enumerated types, range types, and array types. The structure contains comprehensive metadata needed to recreate types and their dependencies.

The structure is populated by the getTypes() function which queries pg_type and related catalogs. It handles various type categories including base types (which may need shell types), domains (with constraints), composite types (backed by relations), arrays, and ranges. Special handling is provided for auto-generated array types and multirange types.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing metadata like catalog ID, dump ID, name, namespace, and dump components
- `dacl`: DumpableAcl structure with ACL string, default ACL, privilege type, and initial privileges
- `ftypname`: Formatted type name from format_type(), which may be quoted and schema-qualified (populated as needed)
- `rolname`: String name of the type owner role
- `typelem`: OID of the element type for arrays and multiranges (from pg_type.typelem)
- `typrelid`: OID of the associated relation for composite types (from pg_type.typrelid)
- `typrelkind`: Relation kind character ('r' for table, 'v' for view, 'c' for composite type, etc.)
- `typtype`: Type category character ('b' for base, 'c' for composite, 'd' for domain, 'e' for enum, 'r' for range, 'm' for multirange)
- `isArray`: Boolean flag indicating if this is an auto-generated array type
- `isMultirange`: Boolean flag indicating if this is an auto-generated multirange type
- `isDefined`: Boolean flag from pg_type.typisdefined indicating if the type is fully defined
- `shellType`: Pointer to associated ShellTypeInfo for base and range types that need shell definitions
- `notnull`: Pointer to not-null constraint for domain types
- `nDomChecks`: Number of CHECK constraints for domain types
- `domChecks`: Array of pointers to CHECK constraints for domain types

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - DumpableAcl (ACL data structure)
  - [ShellTypeInfo](../S/ShellTypeInfo.md) (shell type structure)
  - [ConstraintInfo](../C/ConstraintInfo.md) (constraint structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [getTypes](../g/getTypes.md) (creates and populates TypeInfo arrays)
  - [findTypeByOid](../f/findTypeByOid.md) (searches for TypeInfo by OID)
  - [selectDumpableType](../s/selectDumpableType.md) (determines if type should be dumped)
  - [dumpType](../d/dumpType.md), dumpBaseType, dumpDomain, dumpCompositeType, dumpEnumType, dumpRangeType (output type definitions)
  - [getFormattedTypeName](../g/getFormattedTypeName.md) (formats type names for output)
  - [getDomainConstraints](../g/getDomainConstraints.md) (retrieves domain constraints)

## Notes and Other Information
- Includes both user-defined and built-in types, with built-in types filtered during dump output
- Shell types are created for base and range types to handle dependencies with I/O functions
- Domain types have special constraint handling through notnull and domChecks fields
- Array type detection uses sophisticated logic to identify auto-generated arrays vs. explicitly created ones
- The ftypname field is populated on demand when formatted type names are needed
- Type dumping order is critical due to dependencies between types, functions, and operators
- Located in src/bin/pg_dump/pg_dump.h:197-223