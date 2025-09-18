# publication_invalidation_cb

## Location
src/backend/replication/pgoutput/pgoutput.c: 1768 - 1782

## Overview
publication_invalidation_cb is a callback function that handles syscache invalidation events for the pg_publication system catalog, ensuring publication-related cached data remains consistent.

## Definition


## Detailed Description
publication_invalidation_cb is a syscache invalidation callback function in the pgoutput logical replication output plugin. When changes occur to the pg_publication system catalog (such as creating, dropping, or modifying publications), PostgreSQL's syscache invalidation mechanism calls this function to maintain cache consistency. The function sets the publications_valid flag to false, indicating that cached publication data needs to be refreshed, and also calls rel_sync_cache_publication_cb to invalidate per-relation filtering cache to ensure that relation-specific publication settings are updated on the next access.

## Parameters / Member Variables
- : Datum argument passed by the syscache invalidation system (typically unused in this context)
- : The cache identifier that triggered the invalidation
- : Hash value associated with the invalidated cache entry

## Dependencies
- Functions called/Symbols referenced:
  - [rel_sync_cache_publication_cb](../r/rel_sync_cache_publication_cb.md)
- Called from (representative examples):
  - [pgoutput_startup](pgoutput_startup.md) (registered as callback)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pgoutput.c file
- Part of PostgreSQL's syscache invalidation framework that maintains cache consistency
- Sets the global publications_valid variable to false to force reloading of publication data
- Ensures both publication-level and relation-level caches are properly invalidated
- Registered during pgoutput plugin startup to handle pg_publication catalog changes
- Critical for maintaining data consistency in logical replication scenarios