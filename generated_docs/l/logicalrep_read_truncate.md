# logicalrep_read_truncate

## Location
src/backend/replication/logical/proto.c: 618 - 642

## Overview
Deserializes and reads a TRUNCATE message from the logical replication input stream, parsing the operation details for subscriber processing.

## Definition
```c
List *logicalrep_read_truncate(StringInfo in,
                              bool *cascade, bool *restart_seqs)
```

## Detailed Description
This function is the counterpart to logicalrep_write_truncate, responsible for parsing a TRUNCATE message received through logical replication. It reads the serialized data from the input stream and extracts the number of relations, truncation flags (cascade and restart sequences), and builds a list of relation OIDs that are to be truncated. The function returns the list of relation OIDs while setting the flag parameters through output pointers.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the serialized truncate message to be parsed
- `cascade`: Output parameter - pointer to boolean that will be set to indicate cascade truncation
- `restart_seqs`: Output parameter - pointer to boolean that will be set to indicate sequence restart

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - lappend_oid
  - TRUNCATE_CASCADE
  - TRUNCATE_RESTART_SEQS
- Called from (representative examples):
  - [apply_handle_truncate](../a/apply_handle_truncate.md)

## Notes and Other Information
- Part of the logical replication protocol infrastructure for message deserialization
- Uses PostgreSQL's pq_getmsgint function for binary deserialization
- Flags are decoded using bitwise AND operations with predefined constants
- Returns a List of Oid values representing the relations to be truncated
- Complementary function to logicalrep_write_truncate for protocol communication
- Used by logical replication workers to process incoming truncate operations