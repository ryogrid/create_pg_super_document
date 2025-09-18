# _indxInfo

## Location
src/bin/pg_dump/pg_dump.h: 404 - 424

## Overview
The  structure stores comprehensive metadata about database indexes in pg_dump, including index definitions, storage options, and relationship information for proper index reconstruction.

## Definition


## Detailed Description
The  structure is a comprehensive container for index metadata used by pg_dump to store and reconstruct database indexes. It captures all aspects of an index including its SQL definition, storage characteristics, statistics, partitioning relationships, and constraint associations. This structure enables pg_dump to accurately recreate indexes with all their properties and relationships during database restoration.

## Parameters / Member Variables
- : Base dumpable object information containing metadata such as object ID, name, namespace, and dump ordering information
- : Pointer to the TableInfo structure representing the table that this index belongs to
- : Complete SQL definition of the index (e.g., CREATE INDEX statement)
- : Name of the tablespace where the index is stored
- : Storage options specified with the WITH clause during index creation
- : String representation of column numbers that have associated statistics
- : String representation of statistic values for the corresponding columns
- : Number of key attributes in the index (excludes INCLUDE columns)
- : Total number of attributes in the index (includes both key and INCLUDE columns)
- : Array of OIDs representing both key and non-key attributes, despite the name suggesting only key attributes
- : Boolean flag indicating whether this index is used for table clustering
- : Boolean flag indicating whether this index serves as the replica identity
- : Boolean flag indicating whether NULL values are treated as not distinct in the index
- : OID of the parent index if this index belongs to a partitioned table
- : List of partition attach objects if this is a partitioned index
- : Dump ID of any associated constraint object (e.g., UNIQUE or PRIMARY KEY constraint)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - TableInfo
  - SimplePtrList
  - DumpId
- Called from (representative examples):
  - _tableInfo (referenced in the indexes field)

## Notes and Other Information
This structure is central to pg_dump's index management system and handles complex scenarios including partitioned indexes, index-backed constraints, and various storage options. The  field, despite its name, contains both key and non-key attributes to support PostgreSQL's INCLUDE functionality. The structure also supports replica identity indexes and clustering information, ensuring that all index characteristics are preserved during dump and restore operations. The partitioning support through  and  enables proper reconstruction of partitioned index hierarchies.