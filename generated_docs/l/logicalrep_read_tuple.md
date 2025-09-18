# logicalrep_read_tuple

## Location
src/backend/replication/logical/proto.c: 866 - 925

## Overview
Deserializes tuple (row) data from a logical replication input stream into a LogicalRepTupleData structure, handling various column representations including NULL, unchanged, text, and binary formats.

## Definition
static void logicalrep_read_tuple(StringInfo in, LogicalRepTupleData *tuple)

## Detailed Description
This function is the counterpart to logicalrep_write_tuple, responsible for parsing serialized tuple data from the logical replication protocol stream. It reads the number of attributes from the stream, allocates memory for column values and status information, then processes each column according to its representation type.

The function handles four different column states:
- LOGICALREP_COLUMN_NULL: Column contains NULL value
- LOGICALREP_COLUMN_UNCHANGED: Column value hasn't changed (used in UPDATE operations)
- LOGICALREP_COLUMN_TEXT: Column data in text format
- LOGICALREP_COLUMN_BINARY: Column data in binary format

For text and binary columns, it reads the length, allocates memory, copies the data, and ensures proper null termination for text processing. The parsed data is stored in StringInfo structures within the LogicalRepTupleData for later processing by the subscriber.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the serialized tuple data to parse
- `tuple`: Pointer to LogicalRepTupleData structure to populate with the parsed data

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint: Reads integers from message stream (attribute count and data length)
  - pq_getmsgbyte: Reads single byte (column status/kind)
  - pq_copymsgbytes: Copies data bytes from message stream
  - palloc/palloc0: Allocates memory for column data and status arrays
  - initStringInfoFromString: Initializes StringInfo structure with data
  - LogicalRepTupleData: Structure for storing parsed tuple information
  - LOGICALREP_COLUMN_* constants: Column representation type identifiers

- Called from (representative examples):
  - logicalrep_read_insert: For parsing INSERT operation tuples
  - logicalrep_read_update: For parsing UPDATE operation tuples (old and new)
  - logicalrep_read_delete: For parsing DELETE operation tuples

## Notes and Other Information
- Static function, only accessible within the logical replication protocol module
- Allocates memory for column values and status arrays based on attribute count
- Ensures null termination for both text and binary data for compatibility with input functions
- Handles unchanged columns efficiently by not reading unnecessary data
- Part of the logical replication wire protocol deserialization infrastructure
- Critical for reconstructing tuple data on the subscriber side
- Memory allocation uses palloc0 for colvalues to zero-initialize unused StringInfoData structures
- Error handling for unrecognized column representation types