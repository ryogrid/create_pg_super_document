# getPublicationTables

## Location
src/bin/pg_dump/pg_dump.c: 4522 - 4653

## Overview
Retrieves information about publication membership for dumpable tables, creating objects that represent the relationship between publications and specific tables in PostgreSQL.

## Definition


## Detailed Description
This function queries the  system catalog to collect information about which tables are included in publications. It creates  objects for each publication-table relationship that should be dumped. The function handles version-specific features, supporting row filters (prrelqual) and column lists (prattrs) for PostgreSQL 15.0 and later, while maintaining compatibility with earlier versions (10.0+).

The function filters results based on dump options and only processes relationships where both the publication and table are of interest to the dump operation. Each qualifying relationship results in a  dumpable object.

## Parameters / Member Variables
- : Archive structure containing dump configuration and state information
- : Array of table information structures
- : Number of tables in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) - executes the catalog query
  - [findPublicationByOid](../f/findPublicationByOid.md) - looks up publication info by OID
  - [findTableByOid](../f/findTableByOid.md) - looks up table info by OID
  - [AssignDumpId](../A/AssignDumpId.md) - assigns unique dump ID to the object
  - [selectDumpablePublicationObject](../s/selectDumpablePublicationObject.md) - determines if object should be dumped
  - pg_malloc - allocates memory for publication relation info array
  - atooid - converts string to OID
  - [parsePGArray](../p/parsePGArray.md) - parses PostgreSQL array format for column lists
  - [fmtId](../f/fmtId.md) - formats identifiers safely
  - [pg_strdup](../p/pg_strdup.md) - duplicates strings safely
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) - part of the schema discovery process

## Notes and Other Information
- Only active when  option is not set and PostgreSQL version >= 10.0
- Creates  type dumpable objects
- Supports row filters and column lists for PostgreSQL 15.0+
- Skips relationships where either the publication or table is not being dumped
- Only processes tables whose definitions are being dumped (DUMP_COMPONENT_DEFINITION)
- Memory allocation may be more than needed as it allocates for all tuples before filtering
- Handles NULL values for row filters and column attributes appropriately