# pgstat_release_matching_entry_refs

## Location
[src/backend/utils/activity/pgstat_shmem.c:737-766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L737-L766)

## Overview
A static function that releases statistics entry references that match specific criteria defined by a callback function.

## Definition

```c
static void
pgstat_release_matching_entry_refs(bool discard_pending, ReleaseMatchCB match,
								   Datum match_data)
```
## Detailed Description
This function provides a flexible mechanism for releasing multiple statistics entry references based on custom matching criteria. It iterates through all entries in the local entry reference hash table and applies a user-provided callback function to determine which entries should be released. This design allows for selective cleanup of entry references based on various criteria such as database ID, object type, or other attributes.

The function accepts a callback function () that receives each hash table entry and additional matching data (). If the callback returns true for an entry, that entry is released. The  parameter controls whether pending statistics data should be discarded when releasing the entry reference.

If no hash table exists or if the match callback is NULL, the function handles these cases gracefully by either returning early or processing all entries respectively.

## Parameters / Member Variables
- : Boolean flag indicating whether to discard pending statistics data when releasing entry references
- : Callback function pointer of type  that determines which entries to release
- : Additional data passed to the match callback function as context

## Dependencies
- Functions called/Symbols referenced:
  -  - Starts iteration over the entry reference hash table
  -  - Gets the next entry during hash table iteration
  -  - Releases and cleans up an entry reference
  -  - Hash table entry structure
  -  - Callback function type for matching entries
  -  - Generic data type for passing match context
  -  - Local hash table of entry references

- Called from (representative examples):
  -  - Hash table declaration macro that may reference this function
  -  - Releases all entry references (likely passes NULL for match)
  -  - Releases entry references for a specific database

## Notes and Other Information
- This is a static function, only accessible within the same source file
- The callback-based design provides maximum flexibility for different release scenarios
- Contains an assertion to verify that entry references are not NULL
- Returns early if no hash table exists, making it safe to call in various contexts
- The  parameter allows control over whether to preserve or discard uncommitted statistics data
- Used as a building block for more specific release functions like database-specific or global cleanup
- Located in 