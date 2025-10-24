# findNamespace

## Location
[src/bin/pg_dump/pg_dump.c:5754-5771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5754-L5771)

## Overview
A utility function that looks up a NamespaceInfo structure by OID, providing error handling for missing namespaces.

## Definition

```c
static NamespaceInfo *
findNamespace(Oid nsoid)
```
## Detailed Description
This is a simple wrapper function around findNamespaceByOid that adds error handling. It ensures that when a namespace is looked up by OID, the operation either succeeds or fails with a clear error message. This function is used throughout pg_dump when processing database objects that reference namespaces, ensuring that all namespace references can be resolved to valid NamespaceInfo structures.

The function provides a fatal error if the namespace cannot be found, which indicates a serious inconsistency in the database metadata that would prevent a successful dump operation.

## Parameters / Member Variables
- `nsoid`: The OID of the namespace to find
## Dependencies
- Functions called/Symbols referenced:
  - [findNamespaceByOid](findNamespaceByOid.md)
  - [pg_fatal](../p/pg_fatal.md) (for error reporting)
- Called from (representative examples):
  - fmtQualifiedDumpable
  - [getTypes](../g/getTypes.md)
  - [getOperators](../g/getOperators.md)
  - [getCollations](../g/getCollations.md)
  - [getConversions](../g/getConversions.md)
  - [getOpclasses](../g/getOpclasses.md)
  - [getOpfamilies](../g/getOpfamilies.md)
  - [getAggregates](../g/getAggregates.md)
  - [getFuncs](../g/getFuncs.md)
  - [getTables](../g/getTables.md)

## Notes and Other Information
- Provides a fatal error exit if namespace is not found, ensuring dump consistency
- Used extensively throughout pg_dump for namespace resolution
- Simple wrapper that adds error checking to findNamespaceByOid
- Part of the object lookup infrastructure in pg_dump
- Helps maintain referential integrity during the dump process

## Simplified Source

```c
static NamespaceInfo *
findNamespace(Oid nsoid)
{
    NamespaceInfo *nsinfo;

    // Look up namespace by OID
    nsinfo = findNamespaceByOid(nsoid);

    // Fatal error if not found - ensures dump consistency
    if (nsinfo == NULL)
        pg_fatal("schema with OID %u does not exist", nsoid);

    return nsinfo;
}
```