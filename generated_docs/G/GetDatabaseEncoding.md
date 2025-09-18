# GetDatabaseEncoding

## Location
[src/backend/utils/mb/mbutils.c:1261-1266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1261-L1266)

## Overview
Returns the encoding identifier for the current database, representing the encoding used for storing text-like data types in the database.

## Definition
int GetDatabaseEncoding(void)

## Detailed Description
This function provides access to the database encoding (also called server encoding), which determines how textual data is stored and interpreted in the database. The database encoding affects all text-like data types including cstring, text, varchar, name, xml, and json.

The function is a simple accessor that returns the encoding field from the global DatabaseEncoding structure. This encoding is set during database initialization and remains constant throughout the database session.

The database encoding is fundamental to PostgreSQL's character set handling, as it determines how text data is stored on disk and serves as the reference encoding for character set conversions when communicating with clients using different encodings.

## Parameters / Member Variables
None - this is a parameter-less function.

## Dependencies
- Functions called/Symbols referenced:
  - DatabaseEncoding (global structure containing encoding information)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (src/backend/access/transam/parallel.c:1437)
  - [CollationGetCollid](../C/CollationGetCollid.md) (src/backend/catalog/namespace.c:2375)
  - [DefineCollation](../D/DefineCollation.md) (src/backend/commands/collationcmds.c:256)
  - [BeginCopyFrom](../B/BeginCopyFrom.md) (src/backend/commands/copyfrom.c:1530)
  - [pg_bind_textdomain_codeset](../p/pg_bind_textdomain_codeset.md) (src/backend/utils/mb/mbutils.c:1229)
  - [SetClientEncoding](../S/SetClientEncoding.md) (src/backend/utils/mb/mbutils.c:224)

## Notes and Other Information
- Returns an integer encoding identifier (not a string name)
- The encoding value corresponds to PostgreSQL's internal encoding constants (e.g., PG_UTF8, PG_LATIN1, etc.)
- This is one of the most frequently called functions in PostgreSQL's encoding system
- The database encoding is established at database creation time and cannot be changed afterward
- Used throughout the system for character set conversions, collation operations, and text processing
- Critical for ensuring data consistency across different client encodings