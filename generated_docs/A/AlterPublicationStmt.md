# AlterPublicationStmt

## Location
[src/include/nodes/parsenodes.h:4176-4192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4176-L4192)

## Overview
A parse tree node structure representing an ALTER PUBLICATION SQL statement, which modifies an existing publication's configuration or object membership in PostgreSQL's logical replication system.

## Definition

```c
typedef struct AlterPublicationStmt
{
	NodeTag		type;
	char	   *pubname;		/* Name of the publication */

	/* parameters used for ALTER PUBLICATION ... WITH */
	List	   *options;		/* List of DefElem nodes */

	/*
	 * Parameters used for ALTER PUBLICATION ... ADD/DROP/SET publication
	 * objects.
	 */
	List	   *pubobjects;		/* Optional list of publication objects */
	bool		for_all_tables; /* Special publication for all tables in db */
	AlterPublicationAction action;	/* What action to perform with the given
									 * objects */
} AlterPublicationStmt;
```
## Detailed Description
AlterPublicationStmt represents the parsed form of ALTER PUBLICATION statements, which allow modification of existing publications in PostgreSQL's logical replication framework. This structure supports two main categories of alterations:

1. **Option modifications** (ALTER PUBLICATION ... WITH): Changes publication-level settings such as which DML operations to replicate
2. **Object membership changes** (ALTER PUBLICATION ... ADD/DROP/SET): Modifies which tables or schemas are included in the publication

The structure uses the  field to differentiate between three types of object operations:
- : Add new objects to the publication
- : Remove objects from the publication  
- : Replace the entire object list with new objects

For all-tables publications, the  flag indicates operations on the publication's all-tables status.

## Parameters / Member Variables
- : Standard NodeTag identifier for the parse tree node system
- : String containing the name of the publication being altered; must reference an existing publication
- : List of DefElem nodes containing publication options for WITH clause modifications (e.g., publish settings, publish_via_partition_root)
- : Optional list of PublicationObjSpec nodes specifying which database objects to add, drop, or set; used with ADD/DROP/SET object operations
- : Boolean flag used when converting to/from all-tables publication mode; indicates whether the operation involves all tables in the database
- : AlterPublicationAction enum value specifying the type of object operation (AP_AddObjects, AP_DropObjects, or AP_SetObjects)

## Dependencies
- Functions called/Symbols referenced:
  - AlterPublicationAction (enum defining the type of alteration)
  - NodeTag (standard parse tree node identifier)
  - List (PostgreSQL's generic list structure)
  - DefElem (definition element for options)
  - PublicationObjSpec (implicit, through pubobjects list)
- Called from (representative examples):
  - AlterPublicationOptions (in publicationcmds.c for option modifications)
  - AlterPublicationTables (in publicationcmds.c for table operations)
  - AlterPublicationSchemas (in publicationcmds.c for schema operations)
  - AlterPublication (in publicationcmds.c for main execution logic)
  - ProcessUtilitySlow (in utility.c for statement processing)

## Notes and Other Information
- This structure handles both configuration changes (via options) and membership changes (via action and pubobjects)
- The SET action replaces all current objects with the specified list, while ADD and DROP modify the existing membership
- When altering between regular and all-tables publications, special handling is required for the for_all_tables flag
- ALTER PUBLICATION requires ownership of the publication or superuser privileges
- Object operations (ADD/DROP/SET) can work with individual tables, entire schemas, or mixed combinations
- The structure supports atomic operations - either all specified changes succeed or the entire statement fails
- Publication alterations may trigger invalidation messages to subscribers to refresh their cached metadata