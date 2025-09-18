# logicalrep_write_tuple

## Location
src/backend/replication/logical/proto.c: 769 - 865

## Overview
Serializes a PostgreSQL tuple (row) to the logical replication output stream in the most efficient format possible, supporting both binary and text representations based on column selection.

## Definition
static void logicalrep_write_tuple(StringInfo out, Relation rel, TupleTableSlot *slot, bool binary, Bitmapset *columns)

## Detailed Description
This function is a core component of PostgreSQL's logical replication protocol, responsible for efficiently serializing tuple data. It examines each column in the tuple, filtering out dropped and generated columns, and only processes columns specified in the provided bitmapset. The function optimizes data transmission by:

1. Detecting and marking NULL values with LOGICALREP_COLUMN_NULL
2. Identifying unchanged TOAST data with LOGICALREP_COLUMN_UNCHANGED to avoid retransmitting large values
3. Using binary format (LOGICALREP_COLUMN_BINARY) when possible and requested for better performance
4. Falling back to text format (LOGICALREP_COLUMN_TEXT) when binary is not available or requested

The function first counts live attributes to write the tuple header, then processes each column's value according to its type and the chosen serialization strategy.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized tuple data will be written
- `rel`: Relation object providing metadata about the table structure
- `slot`: TupleTableSlot containing the actual tuple data to serialize
- `binary`: Boolean flag indicating whether to prefer binary serialization when available
- `columns`: Bitmapset specifying which columns to include in the output

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr: Gets relation descriptor for column metadata
  - TupleDescAttr: Accesses attribute information from tuple descriptor
  - [column_in_column_list](../c/column_in_column_list.md): Checks if a column should be included
  - [pq_sendint16](../p/pq_sendint16.md): Writes the count of live attributes
  - slot_getallattrs: Extracts all attribute values from the tuple slot
  - [SearchSysCache1](../S/SearchSysCache1.md): Looks up type information for each column
  - [OidSendFunctionCall](../O/OidSendFunctionCall.md): Calls binary output function for types
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md): Calls text output function for types
  - [pq_sendbyte](../p/pq_sendbyte.md)/pq_sendint/pq_sendbytes/pq_sendcountedtext: Protocol writing functions
  - VARATT_IS_EXTERNAL_ONDISK: Macro to detect unchanged TOAST data
  - Form_pg_type: Structure for type catalog information

- Called from (representative examples):
  - [logicalrep_write_insert](logicalrep_write_insert.md): For INSERT operations
  - logicalrep_write_update: For UPDATE operations (old and new tuples)
  - logicalrep_write_delete: For DELETE operations

## Notes and Other Information
- Static function, only accessible within the logical replication protocol module
- Handles special cases like NULL values, unchanged TOAST data, and dropped/generated columns
- Supports both binary and text serialization formats for optimal performance
- Part of the logical replication wire protocol implementation
- Critical for data consistency between publisher and subscriber in logical replication
- Optimizes bandwidth usage by avoiding retransmission of unchanged large objects (TOAST)