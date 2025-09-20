# _extensionInfo

## Location
[src/bin/pg_dump/pg_dump.h:187-194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L187-L194)

## Overview
The  structure represents PostgreSQL extensions in pg_dump, containing extension-specific metadata such as version information, configuration tables, and relocatability attributes needed for proper extension dumping and restoration.

## Definition

```c
typedef struct _extensionInfo
{
	DumpableObject dobj;
	char	   *namespace;		/* schema containing extension's objects */
	bool		relocatable;
	char	   *extversion;
	char	   *extconfig;		/* info about configuration tables */
	char	   *extcondition;
} ExtensionInfo;
```
## Detailed Description
The  structure represents PostgreSQL extensions within the pg_dump framework. Extensions are packaged collections of database objects (functions, types, operators, etc.) that can be easily installed and managed as units. This structure captures all the essential metadata needed to properly dump and restore extensions, including their version, configuration data, and schema placement.

Unlike tables or functions, extensions have special handling requirements in pg_dump because they can contain multiple related objects and may have configuration tables with user data that needs to be preserved across dump/restore cycles. The structure tracks both the extension's metadata and information about any configuration tables that contain user data.

## Parameters / Member Variables
- : Base  structure containing core metadata, identification, dependencies, and component control
- : String containing the name of the schema that contains the extension's objects
- : Boolean flag indicating whether this extension can be moved between different schemas
- : String containing the version of the extension as stored in the database
- : String containing information about configuration tables that belong to this extension
- : String containing condition information for extension configuration tables

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - Extension management functions in pg_dump
  - Used when processing extension objects and their dependencies

## Notes and Other Information
Extensions in PostgreSQL provide a way to package related database objects together for easy installation and management. The  flag determines whether the extension can be moved to different schemas after installation. Configuration tables () are special tables within extensions that contain user data rather than just extension code, requiring special handling during dump/restore to preserve user data while maintaining extension structure. The  field provides additional filtering information for configuration table data during the dump process.