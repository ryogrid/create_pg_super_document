# listCollations

## Location
[src/bin/psql/describe.c:4908-5025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4908-L5025)

## Overview
The  function implements the  psql command for displaying collation information in a PostgreSQL database.

## Definition

```c
bool
listCollations(const char *pattern, bool verbose, bool showSystem)
```
## Detailed Description
This function queries the  system catalog to retrieve and display information about collations defined in the database. Collations define the rules for sorting and comparing text data in different languages and locales. The function shows comprehensive collation information including schema, name, provider, locale settings, ICU rules, and deterministic properties.

The function includes extensive version-specific logic to handle the evolution of collation features across PostgreSQL versions:
- Provider information handling (PostgreSQL 10+)
- Locale column naming changes (PostgreSQL 15+ uses , PostgreSQL 17+ uses )
- ICU rules support (PostgreSQL 16+)
- Deterministic collation support (PostgreSQL 12+)

The query filters collations based on the current database encoding to show only usable collations and can optionally exclude system collations.

## Parameters / Member Variables
- : A SQL name pattern (with optional wildcards) to filter which collations to display. If NULL, all visible collations are shown.
- : If true, includes collation descriptions from the  catalog in the output.
- : If true, includes system collations from  and  schemas; if false, excludes them (unless a pattern is specified).

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Appends formatted text to the buffer
  - : Validates and processes SQL name patterns with wildcards
  - : Executes the constructed SQL query
  - : Formats and displays the query results with column translation
  - : Cleans up the string buffer
  - : Macro to get array length
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Uses selective column translation for internationalization
- Automatically filters out collations that are incompatible with the current database encoding
- Provider types include: 'default', 'builtin', 'libc', and 'icu' (PostgreSQL 10+)
- Shows different locale information based on PostgreSQL version and provider type
- ICU Rules column shows custom ICU sorting rules for ICU collations (PostgreSQL 16+)
- Deterministic property indicates whether the collation provides consistent, reproducible results
- Results are ordered by schema name and collation name
- System collation filtering respects the pattern parameter - if a pattern is provided, system collations may still be shown if they match