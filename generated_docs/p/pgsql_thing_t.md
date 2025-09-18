# pgsql_thing_t

## Location
src/bin/psql/tab-complete.c: 1225 - 1226

## Overview
A structure used in PostgreSQL's psql tab completion system to define "things" that can appear after CREATE, DROP, or ALTER statements along with their associated query information.

## Definition


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
  - VersionedQuery (structure type)
  - SchemaQuery (structure type)
  - bits32 (type definition)
- Called from (representative examples):
  - words_after_create (static array)
  - HeadMatchesCS (completion function)

## Notes and Other Information
- The structure enforces that at most one of the three query types (query, vquery, squery) should be non-NULL
- Flag constants include THING_NO_CREATE, THING_NO_DROP, THING_NO_ALTER, and THING_NO_SHOW (combination of all three)
- Used extensively in the words_after_create array to define all completable PostgreSQL object types
- Part of psql's sophisticated tab completion system that provides context-aware suggestions based on SQL command context