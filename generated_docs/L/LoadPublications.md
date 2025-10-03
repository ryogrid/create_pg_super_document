# LoadPublications

## Location
[src/backend/replication/pgoutput/pgoutput.c:1746-1767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1746-L1767)

## Overview
LoadPublications loads Publication objects from a list of publication names, used in PostgreSQL logical replication to retrieve publication metadata.

## Definition

```c
static List *
LoadPublications(List *pubnames)
```
## Detailed Description
LoadPublications is a utility function in the pgoutput logical replication output plugin that takes a list of publication names as strings and converts them into a list of Publication objects. The function iterates through each publication name in the input list, looks up the corresponding Publication object using GetPublicationByName, and builds a new list containing the Publication structures. This is essential for the pgoutput plugin to access publication metadata when processing logical replication changes.

## Parameters / Member Variables
- `*pubnames`: A List of publication names as character strings to be loaded
## Dependencies
- Functions called/Symbols referenced:
  - [GetPublicationByName](../G/GetPublicationByName.md)
  - [Publication](../P/Publication.md) (structure type)
- Called from (representative examples):
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pgoutput.c file
- The function assumes that all publication names in the input list are valid and exist
- Uses the GetPublicationByName function with the second parameter as false, meaning it will raise an error if a publication doesn't exist
- Returns NIL (empty list) if the input list is empty
- Part of the PostgreSQL logical replication infrastructure specifically for the pgoutput plugin