# CatalogId

## Location
src/bin/pg_dump/pg_backup.h: 272 - 273

## Overview
CatalogId is a compact structure used by pg_dump to uniquely identify database objects by their system catalog entries, consisting of a table OID and object OID pair.

## Definition


## Detailed Description
CatalogId represents a fundamental identification mechanism in PostgreSQL's pg_dump utility for uniquely identifying database objects through their entries in the system catalogs. The structure combines two OIDs: the tableoid (identifying which system catalog table contains the object's definition) and the oid (identifying the specific object within that catalog table). This dual-OID approach allows pg_dump to precisely locate and reference any database object during dump and restore operations. The structure is designed to be compact and efficient, with a strict requirement that it contains no unused bytes, making it suitable for use as a hash key or in memory-sensitive operations.

## Parameters / Member Variables
- : OID of the system catalog table that contains the object's defining entry (e.g., pg_class for tables, pg_proc for functions)
- : OID of the specific database object within the system catalog table

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - findObjectByCatalogId
  - findTableByOid
  - findIndexByOid
  - findTypeByOid
  - findFuncByOid
  - recordAdditionalCatalogID
  - ArchiveEntry
  - getExtensionMembership
  - getAdditionalACLs

## Notes and Other Information
CatalogId serves as one of two primary object identification mechanisms in pg_dump, alongside DumpId. While DumpId is a sequential integer counter used internally for efficiency and flexibility, CatalogId provides the authoritative reference to actual database catalog entries. This is essential for interpreting pg_depend entries and maintaining referential integrity during dump operations. The structure's design constraint of containing no unused bytes ensures it can be safely used as a hash key in data structures. CatalogId is particularly important for resolving object dependencies and ensuring correct dump ordering based on PostgreSQL's system catalog relationships.