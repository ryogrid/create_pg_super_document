# complete_from_versioned_query

## Location
[src/bin/psql/tab-complete.c:5168-5183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5168-L5183)

## Overview
Provides tab completion functionality for PostgreSQL commands by selecting an appropriate query from a version-dependent array based on the current server version.

## Definition

```c
static char *
complete_from_versioned_query(const char *text, int state)
```
## Detailed Description
This function implements version-aware tab completion in psql by selecting the appropriate query from a versioned query structure. It traverses through a VersionedQuery array (stored in completion_vquery) to find the first query that is compatible with the current PostgreSQL server version (pset.sversion). Once a suitable query is found, it delegates the actual completion work to _complete_from_query. This design allows psql to provide accurate completions that match the capabilities and syntax available in different PostgreSQL versions.

## Parameters / Member Variables
- `*text`: The partial text that the user has typed, which needs to be completed
- `state`: The completion state used by readline for generating multiple matches
## Dependencies
- Functions called/Symbols referenced:
  - [_complete_from_query](_complete_from_query.md)
  - [VersionedQuery](../V/VersionedQuery.md) (struct type)
- Called from (representative examples):
  - COMPLETE_WITH_VERSIONED_QUERY_LIST
  - THING_NO_SHOW

## Notes and Other Information
- Returns NULL if the server version is too old to support any of the available queries
- Relies on global variables: completion_vquery, completion_charpp, completion_verbatim, and pset.sversion
- This is part of psql's sophisticated tab completion system that adapts to different PostgreSQL server versions
- The function is static, indicating it's only used within the tab-complete.c file

## Simplified Source

```c
static char *complete_from_versioned_query(const char *text, int state)
{
    const VersionedQuery *vquery = completion_vquery;

    // Find the first query compatible with current server version
    while (pset.sversion < vquery->min_server_version)
        vquery++;

    // If no compatible query found, fail completion
    if (vquery->query == NULL)
        return NULL;

    // Execute the version-appropriate query
    return _complete_from_query(vquery->query,        // Version-specific SQL
                               NULL,                   // No additional version check
                               completion_charpp,      // Result processing
                               completion_verbatim,    // Verbatim flag
                               text,                   // User input
                               state);                 // Readline state
}
```