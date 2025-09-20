# _internalPQconninfoOption

## Location
[src/interfaces/libpq/fe-connect.c:168-187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L168-L187)

## Overview
An internal structure that extends PQconninfoOption with additional private fields for managing PostgreSQL connection parameters within libpq.

## Definition

```c
typedef struct _internalPQconninfoOption
{
	char	   *keyword;		/* The keyword of the option			*/
	char	   *envvar;			/* Fallback environment variable name	*/
	char	   *compiled;		/* Fallback compiled in default value	*/
	char	   *val;			/* Option's current value, or NULL		*/
	char	   *label;			/* Label for field in connect dialog	*/
	char	   *dispchar;		/* Indicates how to display this field in a
								 * connect dialog. Values are: "" Display
								 * entered value as is "*" Password field -
								 * hide value "D"  Debug option - don't show
								 * by default */
	int			dispsize;		/* Field size in characters for dialog	*/
	/* ---
	 * Anything above this comment must be synchronized with
	 * PQconninfoOption in libpq-fe.h, since we memcpy() data
	 * between them!
	 * ---
	 */
	off_t		connofs;		/* Offset into PGconn struct, -1 if not there */
} internalPQconninfoOption;
```
## Detailed Description
This structure is the internal representation of connection information options used by libpq for managing PostgreSQL database connections. It extends the public PQconninfoOption structure with an additional private field () that tracks the offset of each option within the PGconn structure. The first part of this structure is intentionally kept synchronized with PQconninfoOption in libpq-fe.h to allow safe memory copying between the two structures. This design allows libpq to maintain both a public API and internal implementation details while ensuring compatibility.

The structure serves as the foundation for connection parameter management, supporting fallback mechanisms through environment variables and compiled-in defaults, and providing metadata for GUI applications that want to create database connection dialogs.

## Parameters / Member Variables
- : The name/keyword of the connection option (e.g., "host", "port", "dbname")
- : Name of the environment variable to check for fallback value (e.g., "PGHOST", "PGPORT")
- : Compiled-in default value used when no other value is available
- : Current value of the option, or NULL if not set
- : Human-readable label for the option, used in connection dialogs
- : Display character indicator for GUI applications:
  - : Normal input field
  - : Password field (hide value)
  - : Debug option (don't show by default)
- : Suggested field size in characters for dialog display
- : Offset into the PGconn structure where this option's value is stored, or -1 if not stored there

## Dependencies
- Functions called/Symbols referenced:
  - [PQconninfoOption](../P/PQconninfoOption.md) (public counterpart structure)
  - PGconn (connection structure where values are stored)
- Called from (representative examples):
  - Connection parameter processing functions
  - PQconndefaults() and related functions

## Notes and Other Information
- Critical synchronization requirement: The first 7 fields must remain identical to PQconninfoOption in libpq-fe.h
- Memory management: Non-null  fields point to malloc'd strings that must be freed appropriately
- The  field enables efficient mapping between connection options and the actual PGconn structure fields
- Used internally by libpq for processing connection strings, environment variables, and default values
- Located in 