# collectComments

## Location
src/bin/pg_dump/pg_dump.c: 10437 - 10521

## Overview
Constructs a table of all comments available for database objects and sets the has-comment component flag for each relevant object in pg_dump.

## Definition


## Detailed Description
The  function retrieves all comments from the  system catalog in a single query and builds an in-memory lookup table. This approach is much more efficient than performing per-object queries for comments. The function also sets the  flag on dumpable objects that have associated comments, which helps the dump process know which objects require comment handling.

The collected comments are stored in a global array  sorted by  for fast lookup during the dump process. The function handles a special case for composite types where column comments are linked to the type's pg_class entry but the flag needs to be set on the type's own DumpableObject.

## Parameters / Member Variables
- : Archive structure representing the dump destination and containing connection information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [findObjectByCatalogId](../f/findObjectByCatalogId.md)
  - [findTypeByOid](../f/findTypeByOid.md)
  - pg_malloc
  - atooid
  - createPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - destroyPQExpBuffer
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c:991)

## Notes and Other Information
- Uses a single SQL query to fetch all comments at once for performance optimization
- Results are sorted by classoid, objoid, objsubid to enable efficient binary search lookups
- Special handling for composite type column comments that need flag propagation to the parent type
- Memory allocation is done upfront for the entire comments array based on query result count
- Only comments for dumpable objects are retained in the final comments array