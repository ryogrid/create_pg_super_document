# compareentry

## Location
[src/backend/utils/adt/tsvector.c:87-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector.c#L87-L102)

## Overview
A comparison function for sorting WordEntry structures based on their string content in PostgreSQL's text search functionality.

## Definition
```c
static int compareentry(const void *va, const void *vb, void *arg)
```

## Detailed Description
This function serves as a comparator for qsort_arg operations on arrays of WordEntry structures. It compares two WordEntry structures by their associated string content, using the buffer string passed as the third argument to access the actual text data. The function leverages PostgreSQL's tsCompareString function to perform case-insensitive string comparison. It's designed to work with both WordEntry and WordEntryIN structures since WordEntryIN has WordEntry as its first field, making the comparison compatible through pointer casting.

## Parameters / Member Variables
- `va`: Pointer to the first WordEntry structure to compare (cast from const void*)
- `vb`: Pointer to the second WordEntry structure to compare (cast from const void*)
- `arg`: Pointer to the buffer string containing the actual text data (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - [tsCompareString](../t/tsCompareString.md) (PostgreSQL's text search string comparison function)
  - [WordEntry](../W/WordEntry.md) (structure type representing a word entry)
- Called from (representative examples):
  - [uniqueentry](../u/uniqueentry.md) (for sorting entries before removing duplicates)
  - [tsvectorrecv](../t/tsvectorrecv.md) (during binary tsvector deserialization)

## Notes and Other Information
- Returns negative, zero, or positive value if the first entry's string is less than, equal to, or greater than the second entry's string respectively
- Follows the qsort_arg comparator function signature which allows passing additional context
- Uses case-insensitive string comparison (false parameter to tsCompareString)
- Essential for maintaining lexicographically sorted word entries in tsvector data structures
- The buffer string argument provides access to the actual text content referenced by WordEntry position and length fields