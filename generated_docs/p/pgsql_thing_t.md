# pgsql_thing_t

## Location
[src/bin/psql/tab-complete.c:1225-1226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L1225-L1226)

## Overview
A structure used in PostgreSQL's psql tab completion system to define "things" that can appear after CREATE, DROP, or ALTER statements along with their associated query information.

## Definition

```c
const bits32 flags;			/* visibility flags, see below */
} pgsql_thing_t;

#define THING_NO_CREATE		(1 << 0)	/* should not show up after CREATE */
#define THING_NO_DROP		(1 << 1)	/* should not show up after DROP */
#define THING_NO_ALTER		(1 << 2)	/* should not show up after ALTER */
#define THING_NO_SHOW		(THING_NO_CREATE | THING_NO_DROP | THING_NO_ALTER)

/* When we have DROP USER etc, also offer MAPPING FOR */
static const char *const Keywords_for_user_thing[] =
```
## Detailed Description
The  structure is a core component of psql's tab completion system, specifically designed to represent database objects that can be created, dropped, or altered. Each instance defines a PostgreSQL object type (like "TABLE", "INDEX", "USER", etc.) along with the appropriate query to retrieve existing instances of that object type for completion suggestions. The structure supports three mutually exclusive query types to accommodate different retrieval strategies, and includes visibility flags to control when the object type should be suggested.

## Parameters / Member Variables
- : The name of the PostgreSQL object type (e.g., "TABLE", "INDEX", "USER")
- : A simple SQL query string to retrieve object names, or NULL if not using this query type
- : A pointer to a versioned query structure for version-dependent queries, or NULL
- : A pointer to a schema query structure for schema-aware queries, or NULL
- : An array of additional keyword strings to offer alongside object names during completion
- : Bitwise flags controlling visibility (uses THING_NO_CREATE, THING_NO_DROP, THING_NO_ALTER constants)

## Dependencies
- Functions called/Symbols referenced:
  - [VersionedQuery](../V/VersionedQuery.md) (structure type)
  - [SchemaQuery](../S/SchemaQuery.md) (structure type)
  - bits32 (type definition)
- Called from (representative examples):
  - words_after_create (static array)
  - HeadMatchesCS (completion function)

## Notes and Other Information
- The structure enforces that at most one of the three query types (query, vquery, squery) should be non-NULL
- Flag constants include THING_NO_CREATE, THING_NO_DROP, THING_NO_ALTER, and THING_NO_SHOW (combination of all three)
- Used extensively in the words_after_create array to define all completable PostgreSQL object types
- Part of psql's sophisticated tab completion system that provides context-aware suggestions based on SQL command context