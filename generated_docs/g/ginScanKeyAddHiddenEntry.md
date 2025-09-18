# ginScanKeyAddHiddenEntry

## Location
[src/backend/access/gin/ginscan.c:142-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginscan.c#L142-L157)

## Overview
Appends a hidden scan entry of a specified category to a GIN scan key, used for handling special search conditions like NULL values.

## Definition


## Detailed Description
The  function adds a special "hidden" scan entry to an existing scan key. These hidden entries are used to handle special search conditions, particularly for NULL value searches and other categorical queries that don't correspond to regular data values.

The function creates a scan entry with InvalidStrategy since strategy numbers are not relevant for these special categorical entries. The hidden entry uses a dummy datum value of 0 and is marked as not being a partial match operation.

This function assumes it will be called at most once per scan key, as  only allocates space for one hidden entry per key.

## Parameters
- : GIN scan opaque data structure containing scan state
- : The scan key to which the hidden entry will be added
- : The category of the hidden query (e.g., GIN_CAT_NULL_KEY)

## Dependencies
- Functions called/Symbols referenced:
  - [ginFillScanEntry](ginFillScanEntry.md)
  - InvalidStrategy (constant)
- Called from:
  - [ginFillScanKey](ginFillScanKey.md) (multiple locations)
  - [ginNewScanKey](ginNewScanKey.md)

## Notes and Other Information
- Should be called at most once per scan key due to space limitations
- Uses InvalidStrategy since strategy is not meaningful for categorical entries
- Uses dummy Datum value of 0 since no actual data value is being searched
- Always creates non-partial-match entries (isPartialMatch = false)
- Part of the GIN index's mechanism for handling special search categories like NULL values