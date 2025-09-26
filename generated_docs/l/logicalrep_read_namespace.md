# logicalrep_read_namespace

## Location
[src/backend/replication/logical/proto.c:1055-1068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L1055-L1068)

## Overview
Reads namespace name information from a logical replication message stream and handles the space-saving optimization for pg_catalog namespace.

## Definition
```c
static const char *logicalrep_read_namespace(StringInfo in)
```

## Detailed Description
This function deserializes namespace (schema) information from a logical replication protocol message. It reads a string from the input buffer and implements the corresponding logic to the logicalrep_write_namespace optimization: when an empty string (null byte) is encountered, it returns "pg_catalog" as the namespace name. This handles the space-saving optimization where pg_catalog namespace names are transmitted as null bytes instead of full strings.

The function provides the decoding counterpart to logicalrep_write_namespace, ensuring that namespace information is correctly reconstructed on the receiving side of logical replication.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the incoming logical replication message data

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgstring](../p/pq_getmsgstring.md) (extract string value from message buffer)
- Called from:
  - [logicalrep_read_rel](logicalrep_read_rel.md) (reads relation information including namespace)
  - [logicalrep_read_typ](logicalrep_read_typ.md) (reads type information including namespace)

## Notes and Other Information
- This is a static function used internally within logical replication protocol implementation
- Returns a const char pointer to either the read string or the literal "pg_catalog" string
- The returned string pointer is valid for the lifetime of the input StringInfo buffer
- Implements the receiving side of the pg_catalog namespace optimization for reduced message size
- Part of PostgreSQL's logical replication subsystem for efficiently receiving schema information
- The function assumes the input message is well-formed and does not perform extensive error checking