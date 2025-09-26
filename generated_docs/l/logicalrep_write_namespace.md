# logicalrep_write_namespace

## Location
[src/backend/replication/logical/proto.c:1035-1054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L1035-L1054)

## Overview
Writes namespace name information to a logical replication message stream, using space-saving optimization for the pg_catalog namespace.

## Definition
```c
static void logicalrep_write_namespace(StringInfo out, Oid nspid)
```

## Detailed Description
This function serializes namespace (schema) information into a logical replication protocol message. It implements a space-saving optimization by writing a null byte for the pg_catalog namespace instead of the full name string, since pg_catalog is the most commonly referenced namespace. For all other namespaces, it looks up the namespace name using the system catalog and writes the full name string to the output buffer.

The function handles error cases where the namespace lookup fails, which could indicate a corrupt catalog or race condition during namespace deletion.

## Parameters / Member Variables
- `out`: StringInfo buffer to write the namespace information to
- `nspid`: OID of the namespace to serialize

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md) (write single byte to message buffer)
  - [pq_sendstring](../p/pq_sendstring.md) (write null-terminated string to message buffer)
  - [get_namespace_name](../g/get_namespace_name.md) (look up namespace name from OID)
  - PG_CATALOG_NAMESPACE (constant for pg_catalog namespace OID)
  - elog (error logging and reporting)
- Called from:
  - [logicalrep_write_rel](logicalrep_write_rel.md) (writes relation information including namespace)
  - [logicalrep_write_typ](logicalrep_write_typ.md) (writes type information including namespace)

## Notes and Other Information
- This is a static function used internally within logical replication protocol implementation
- The pg_catalog optimization reduces message size since most built-in types and relations are in pg_catalog
- The function will terminate the process with ERROR if namespace lookup fails, ensuring data consistency
- Part of PostgreSQL's logical replication subsystem for efficiently transmitting schema information
- The null byte optimization is understood by the corresponding read function to reconstruct pg_catalog namespace name