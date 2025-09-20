# getPublicationNamespaces

## Location
[src/bin/pg_dump/pg_dump.c:4435-4521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4435-L4521)

## Overview
Retrieves information about publication membership for dumpable schemas, creating objects that represent the relationship between publications and namespaces in PostgreSQL.

## Definition

```c
void
getPublicationNamespaces(Archive *fout)
```
## Detailed Description
This function queries the  system catalog to collect information about which schemas are included in publications. It creates  objects for each publication-namespace relationship that should be dumped. The function is part of pg_dump's schema discovery phase and only operates on PostgreSQL version 15.0 and later, as publication namespaces were introduced in that version.

The function filters results based on dump options and only processes relationships where both the publication and namespace are of interest to the dump operation. Each qualifying relationship results in a  dumpable object.

## Parameters / Member Variables
- : Archive structure containing dump configuration and state information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) - executes the catalog query
  - [findPublicationByOid](../f/findPublicationByOid.md) - looks up publication info by OID
  - [findNamespaceByOid](../f/findNamespaceByOid.md) - looks up namespace info by OID
  - [AssignDumpId](../A/AssignDumpId.md) - assigns unique dump ID to the object
  - [selectDumpablePublicationObject](../s/selectDumpablePublicationObject.md) - determines if object should be dumped
  - pg_malloc - allocates memory for publication schema info array
  - atooid - converts string to OID
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) - part of the schema discovery process

## Notes and Other Information
- Only active when  option is not set and PostgreSQL version >= 15.0
- Creates  type dumpable objects
- Skips relationships where either the publication or namespace is not being dumped
- Memory allocation may be more than needed as it allocates for all tuples before filtering