# LogicalRepTupleData

## Location
src/include/replication/logicalproto.h: 84 - 92

## Overview
LogicalRepTupleData is a structure that stores a tuple received via logical replication, where the columns correspond to the remote table structure.

## Definition


## Detailed Description
This structure is the fundamental data container for tuples transmitted through PostgreSQL's logical replication protocol. It represents a single row of data from a remote table, maintaining both the actual column values and metadata about each column's state. The structure is designed to handle variable numbers of columns and different data states that can occur during replication operations.

The colvalues array contains the actual data for each column in StringInfo format, allowing for efficient string manipulation and storage. The colstatus array provides crucial metadata indicating whether each column is null, unchanged, contains text data, or contains binary data. This design enables efficient handling of partial updates and various data types during replication.

## Parameters / Member Variables
- : Array of StringInfoData structures, one per column; stores the actual column data in string format, some entries may be unused for columns that are not being replicated
- : Array of character markers indicating the status of each column (null/unchanged/text/binary format)
- : Integer specifying the length of both the colvalues and colstatus arrays, representing the total number of columns

## Dependencies
- Functions called/Symbols referenced:
  - [StringInfoData](../S/StringInfoData.md) (data structure)
- Called from (representative examples):
  - logicalrep_read_insert
  - logicalrep_read_update  
  - logicalrep_read_delete
  - [logicalrep_read_tuple](../l/logicalrep_read_tuple.md)
  - [apply_handle_insert](../a/apply_handle_insert.md)
  - [apply_handle_update](../a/apply_handle_update.md)
  - [apply_handle_delete](../a/apply_handle_delete.md)
  - [slot_store_data](../s/slot_store_data.md)
  - [slot_modify_data](../s/slot_modify_data.md)

## Notes and Other Information
- This structure is specifically designed for logical replication and should not be confused with regular HeapTuple structures used elsewhere in PostgreSQL
- The columns in this structure correspond to the remote table's schema, not necessarily the local table's schema
- Memory management for the StringInfoData arrays must be handled carefully to avoid leaks
- The structure supports partial column updates through the colstatus markers, which is essential for efficient replication of large tables
- Located in src/include/replication/logicalproto.h:84-92