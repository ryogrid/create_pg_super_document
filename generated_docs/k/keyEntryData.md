# keyEntryData

## Location
[src/backend/access/gin/ginutil.c:433-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L433-L439)

## Overview
A simple structure used for sorting key datums during GIN index key extraction operations, holding both the data value and its null status.

## Definition

```c
typedef struct
{
	FmgrInfo   *cmpDatumFunc;
	Oid			collation;
	bool		haveDups;
} cmpEntriesArg;
```
## Detailed Description
 is a utility structure used specifically within the  function in PostgreSQL's GIN (Generalized Inverted Index) access method. This structure serves as a temporary container for key data during the sorting and deduplication process. When multiple keys are extracted from an indexed value, they need to be sorted and duplicates removed for efficient storage. The  structure pairs each key datum with its null flag, allowing the sorting algorithm to handle both null and non-null values appropriately.

The structure is designed to support the sorting operations required by GIN indexes, where keys must be ordered consistently and duplicates eliminated. It's used as an intermediate representation that can be passed to the  function along with a custom comparison function ().

## Parameters / Member Variables
- : The actual key value data stored as a Datum (PostgreSQL's generic data type)
- : Boolean flag indicating whether the datum represents a NULL value

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL data type)
  - [bool](../b/bool.md) (standard boolean type)
- Called from (representative examples):
  - [cmpEntries](../c/cmpEntries.md) (used in sorting comparison function at src/backend/access/gin/ginutil.c:445-446)
  - [ginExtractEntries](../g/ginExtractEntries.md) (main usage for sorting keys at src/backend/access/gin/ginutil.c:543, 546, 556)

## Notes and Other Information
- This structure is used only for internal sorting operations within GIN index key extraction
- The structure is temporary - data is copied from the original entries array, sorted, and then copied back
- NULL handling is explicit through the  flag, following PostgreSQL's standard approach to null value management
- Used in conjunction with  structure and  comparison function for the sorting process
- The sorting is performed using  to enable duplicate detection and removal
- Only used when there are multiple keys (*nentries > 1) that need sorting and deduplication