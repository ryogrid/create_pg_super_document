# match_db_entries

## Location
[src/backend/utils/activity/pgstat.c:702-713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L702-L713)

## Overview
A static helper function that determines if a statistics hash entry belongs to the current database, used exclusively by pgstat_reset_counters() for database-specific statistics filtering.

## Definition


## Detailed Description
This function serves as a filter predicate for statistics hash table operations. It compares the database OID stored in a statistics hash entry against the current backend's database OID (MyDatabaseId) to determine if the entry belongs to the current database.

The function is designed to work with the PostgreSQL statistics system's hash table traversal mechanisms, where a filtering function is needed to identify entries that should be processed for database-specific operations like statistics reset.

The match_data parameter is not used in the current implementation, as the function relies on the global MyDatabaseId variable to determine the current database context.

## Parameters / Member Variables
- : Pointer to a statistics hash table entry containing the key information including database OID
- : Additional matching data (unused in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetObjectId](../D/DatumGetObjectId.md) (for data type conversion)
  - MyDatabaseId (global variable representing current database OID)
- Called from (representative examples):
  - [pgstat_reset_counters](../p/pgstat_reset_counters.md) (uses this as a filter function)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pgstat.c file
- Designed specifically for use with pgstat_reset_counters() and not intended for general use
- The function follows the standard PostgreSQL pattern of using predicate functions for hash table filtering operations
- Returns true when the entry belongs to the current database, false otherwise
- The unused match_data parameter maintains compatibility with the expected function signature for hash table filter functions