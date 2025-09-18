# InitTableAmRoutine

## Location
[src/backend/utils/cache/relcache.c:1801-1809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L1801-L1809)

## Overview
InitTableAmRoutine fills in the TableAmRoutine structure for a table relation by calling the table access method handler to obtain the access method interface.

## Definition
```c
static void InitTableAmRoutine(Relation relation)
```

## Detailed Description
This static function initializes the TableAmRoutine structure for a table relation. It is a simple wrapper that calls GetTableAmRoutine() with the relation's access method handler (rd_amhandler) to obtain the TableAmRoutine struct and assigns it to the relation's rd_tableam field. This provides the relation with access to all table access method operations such as tuple insertion, deletion, scanning, and other table-level operations.

## Parameters / Member Variables
- `relation`: The table relation for which to initialize the table access method routine. The relation's rd_amhandler must already be valid.

## Dependencies
- Functions called/Symbols referenced:
  - [GetTableAmRoutine](../G/GetTableAmRoutine.md)
- Called from:
  - [RelationInitTableAccessMethod](../R/RelationInitTableAccessMethod.md)

## Notes and Other Information
- This is a static function within relcache.c used for table relation cache initialization
- Much simpler than the corresponding InitIndexAmRoutine as table access methods have a more straightforward interface
- The function assumes that relation->rd_amhandler is already valid and points to the correct access method handler
- Part of PostgreSQL's pluggable table access method infrastructure introduced to support different storage engines
- Essential for table operations as it provides the function pointers needed for all table access method operations
- The rd_tableam field populated by this function is used throughout the system for table access operations