# PUBACTION_DELETE

## Location
[src/backend/replication/pgoutput/pgoutput.c:109-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L109-L111)

## Overview
PUBACTION_DELETE is an enum value representing the delete operation in PostgreSQL's logical replication row filtering system, used to identify and filter DELETE operations during publication.

## Definition


## Detailed Description
PUBACTION_DELETE is the third and final member of the RowFilterPubAction enum, defined in the pgoutput logical replication plugin. This enum value is specifically used to represent DELETE operations when implementing row-level filtering for logical replication publications. The enum serves as an index into various arrays and data structures that manage row filtering logic for different DML operations.

The enum is part of PostgreSQL's logical replication infrastructure, where it helps categorize and process different types of database modification operations. Only three publication actions are used for row filtering (insert, update, delete), making this a complete enumeration of the supported DML operations that can be filtered in logical replication.

## Parameters / Member Variables
- This is an enum constant with no parameters or member variables
- Enum value: 2 (third member of RowFilterPubAction enum)

## Dependencies
- Functions called/Symbols referenced:
  - None (enum constant)
- Called from (representative examples):
  - NUM_ROWFILTER_PUBACTIONS (macro calculation)
  - [pgoutput_row_filter_init](../p/pgoutput_row_filter_init.md) (row filter initialization)
  - [pgoutput_row_filter](../p/pgoutput_row_filter.md) (row filtering logic)
  - map_changetype_pubaction array (change type mapping)

## Notes and Other Information
- Used as an array index in row filtering data structures, requiring careful bounds checking
- Part of a mapping system that converts REORDER_BUFFER_CHANGE_DELETE to PUBACTION_DELETE
- The enum value is used to index into various arrays including no_filter[], rfnodes[], and map_changetype_pubaction[]
- Critical for logical replication row filtering where DELETE operations need to be identified and potentially filtered based on publication settings
- The NUM_ROWFILTER_PUBACTIONS macro uses this value (+1) to define the total number of supported publication actions
- Located in src/backend/replication/pgoutput/pgoutput.c:109