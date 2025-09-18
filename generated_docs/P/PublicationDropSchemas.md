# PublicationDropSchemas

## Location
[src/backend/commands/publicationcmds.c:1854-1887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1854-L1887)

## Overview
Removes a list of schemas from an existing PostgreSQL publication by deleting their publication-namespace mappings from the system catalog.

## Definition
```c
static void PublicationDropSchemas(Oid pubid, List *schemas, bool missing_ok)
```

## Detailed Description
This static function iterates through a list of schema OIDs and removes each schema from the specified publication by deleting the corresponding entries from the pg_publication_namespace system catalog. The function first looks up the publication-namespace mapping OID using the system cache, then performs a cascaded deletion of the mapping object. It provides flexible error handling through the missing_ok parameter, allowing callers to choose whether missing schemas should cause an error or be silently ignored.

The function uses the system cache PUBLICATIONNAMESPACEMAP to efficiently locate publication-namespace relationships and employs the standard PostgreSQL object deletion mechanism with CASCADE semantics to ensure proper cleanup of dependent objects.

## Parameters / Member Variables
- `pubid`: OID of the publication from which schemas will be removed
- `schemas`: List of schema OIDs to be removed from the publication
- `missing_ok`: Boolean flag controlling error behavior when a schema is not found in the publication (true = ignore missing schemas, false = raise error)

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid2
  - [get_namespace_name](../g/get_namespace_name.md)
  - ObjectAddressSet
  - [performDeletion](../p/performDeletion.md)
  - DROP_CASCADE (constant)
- Called from (representative examples):
  - [AlterPublicationSchemas](../A/AlterPublicationSchemas.md)

## Notes and Other Information
- This is a static function, only accessible within the publicationcmds.c compilation unit
- Uses PUBLICATIONNAMESPACEMAP system cache for efficient lookup of publication-namespace mappings
- Performs CASCADE deletion to ensure proper cleanup of any dependent objects
- Error messages reference "tables from schema" to provide user-friendly context about what is being removed
- The function handles both strict (error on missing) and permissive (ignore missing) deletion modes
- Each successful removal involves looking up the pg_publication_namespace entry and deleting it through the standard object deletion infrastructure