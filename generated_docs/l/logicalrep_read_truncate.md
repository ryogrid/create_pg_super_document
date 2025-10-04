# logicalrep_read_truncate

## Location
[src/backend/replication/logical/proto.c:618-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L618-L642)

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
  - [lappend_oid](lappend_oid.md)
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

## Simplified Source

```c
List *logicalrep_read_truncate(StringInfo in, bool *cascade, bool *restart_seqs) {
    int i;
    int nrelids;
    List *relids = NIL;
    uint8 flags;

    // Read number of relations
    nrelids = pq_getmsgint(in, 4);

    // Read and decode truncate flags
    flags = pq_getmsgint(in, 1);
    *cascade = (flags & TRUNCATE_CASCADE) > 0;
    *restart_seqs = (flags & TRUNCATE_RESTART_SEQS) > 0;

    // Read all relation OIDs and build list
    for (i = 0; i < nrelids; i++)
        relids = lappend_oid(relids, pq_getmsgint(in, 4));

    return relids;
}
```