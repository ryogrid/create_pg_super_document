# logicalrep_read_typ

## Location
src/backend/replication/logical/proto.c: 756 - 768

## Overview
Reads and deserializes PostgreSQL type information from a logical replication input stream into a LogicalRepTyp structure.

## Definition
void logicalrep_read_typ(StringInfo in, LogicalRepTyp *ltyp)

## Detailed Description
This function is the counterpart to logicalrep_write_typ, responsible for parsing type metadata messages in the logical replication protocol. It reads a serialized type information message from the input stream and populates a LogicalRepTyp structure with the remote type ID, namespace name, and type name. This allows logical replication subscribers to understand the data types used by the publisher.

The function extracts the type's remote OID and qualified name (namespace + type name) from the stream, enabling the subscriber to map remote types to local equivalents or create appropriate type mappings.

## Parameters / Member Variables
- `in`: StringInfo buffer containing the serialized type information to read
- `ltyp`: Pointer to LogicalRepTyp structure that will be populated with the type information

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint: Reads 32-bit integer (remote type OID) from the message
  - logicalrep_read_namespace: Reads namespace information from the stream
  - pq_getmsgstring: Reads null-terminated string (type name) from the message
  - pstrdup: Duplicates strings into current memory context
  - LogicalRepTyp: Structure representing logical replication type information

- Called from (representative examples):
  - apply_handle_type (in logical replication worker)

## Notes and Other Information
- Works in conjunction with logicalrep_write_typ to implement the type information protocol
- The LogicalRepTyp structure stores remote type information for mapping to local types
- Memory for namespace and type names is allocated in the current memory context using pstrdup
- Part of the logical replication message processing infrastructure
- Essential for maintaining type consistency between publisher and subscriber databases