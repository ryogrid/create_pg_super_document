# OpenTableList

## Location
[src/backend/commands/publicationcmds.c:1549-1698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1549-L1698)

## Overview
Opens and locks relations specified by a PublicationTable list, preparing them for addition to a publication with proper validation and inheritance handling.

## Definition

```c
static List *
OpenTableList(List *tables)
```
## Detailed Description
OpenTableList is a static function that processes a list of PublicationTable structures to open and lock the corresponding database relations. The function performs several critical tasks:

1. **Relation Opening and Locking**: Opens each specified table with ShareUpdateExclusiveLock to prevent concurrent modifications during publication operations
2. **Duplicate Detection**: Implements an O(N^2) algorithm to filter out duplicate table specifications while ensuring no conflicts exist with WHERE clauses or column lists
3. **Inheritance Handling**: For tables with inheritance (when inh flag is set), automatically includes child tables, except for partitioned tables whose partitions need not be explicitly added
4. **Validation**: Ensures no conflicting WHERE clauses or column lists exist between parent and child tables or duplicate entries

The function returns a list of PublicationRelInfo structures containing the opened relations along with their associated WHERE clauses and column lists.

## Parameters / Member Variables
- : List of PublicationTable structures specifying the tables to be opened, each containing relation information, optional WHERE clauses, and column lists

## Dependencies
- Functions called/Symbols referenced:
  - [table_openrv](../t/table_openrv.md) (opens relation by RangeVar)
  - [find_all_inheritors](../f/find_all_inheritors.md) (finds child tables for inheritance)
  - [list_member_oid](../l/list_member_oid.md) (checks for duplicate OIDs)
  - [lappend](../l/lappend.md)/lappend_oid (list manipulation)
  - RelationGetRelid (gets relation OID)
  - RelationGetRelationName (gets relation name)
- Called from (representative examples):
  - [CreatePublication](../C/CreatePublication.md) (src/backend/commands/publicationcmds.c:831)
  - [AlterPublicationTables](../A/AlterPublicationTables.md) (src/backend/commands/publicationcmds.c:1095)

## Notes and Other Information
- Uses ShareUpdateExclusiveLock to ensure safe concurrent access during publication operations
- The duplicate detection algorithm is O(N^2) but considered acceptable for user-specified table lists
- Inheritance handling excludes partitioned tables as their partitions are handled separately
- Proper error handling for conflicting WHERE clauses and column lists between parent and child tables
- Memory allocation using palloc for PublicationRelInfo structures
- Includes CHECK_FOR_INTERRUPTS() calls to allow query cancellation during long operations