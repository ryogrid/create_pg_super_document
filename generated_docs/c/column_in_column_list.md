# column_in_column_list

## Location
[src/backend/replication/logical/proto.c:50-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L50-L59)

## Overview
A static utility function that checks if a specified column is covered by a column list, used in PostgreSQL's logical replication protocol implementation.

## Definition

```c
static bool
column_in_column_list(int attnum, Bitmapset *columns)
```
## Detailed Description
This function determines whether a given column (identified by its attribute number) is included in a specified column list represented as a Bitmapset. The function handles a special case where a NULL column list is interpreted as covering all columns, effectively meaning no column filtering is applied.

The function is used within PostgreSQL's logical replication protocol to determine which columns should be included when writing tuple data or attribute information to the replication stream.

## Parameters / Member Variables
- `attnum`: The attribute number (column number) to check for inclusion in the column list
- `*columns`: A Bitmapset pointer representing the column list; NULL means all columns are included
## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md) (checks if a member is present in a Bitmapset)
- Called from (representative examples):
  - [logicalrep_write_tuple](../l/logicalrep_write_tuple.md) (multiple locations for filtering columns during tuple serialization)
  - [logicalrep_write_attrs](../l/logicalrep_write_attrs.md) (multiple locations for filtering attributes during attribute information serialization)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the proto.c file
- The NULL handling is crucial for backwards compatibility and represents "all columns included"
- Used extensively in logical replication tuple and attribute serialization to support column-level filtering
- Part of PostgreSQL's logical replication protocol implementation located in src/backend/replication/logical/proto.c

## Simplified Source

```c
static bool column_in_column_list(int attnum, Bitmapset *columns) {
    // NULL columns means all columns are included
    // Otherwise check if the attribute is in the bitmap
    return (columns == NULL || bms_is_member(attnum, columns));
}
```