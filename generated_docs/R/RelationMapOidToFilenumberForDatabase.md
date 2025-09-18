# RelationMapOidToFilenumberForDatabase

## Location
src/backend/utils/cache/relmapper.c: 265 - 291

## Overview
Maps a relation OID to its file number by reading the relation mapping file from a specified database path, rather than using the current database's mapping.

## Definition
RelFileNumber RelationMapOidToFilenumberForDatabase(char *dbpath, Oid relationId)

## Detailed Description
This function provides OID-to-filenumber mapping capability for relations in databases other than the current one. Unlike RelationMapOidToFilenumber which operates on the current database's in-memory mapping tables, this function reads the relation mapping file directly from disk at the specified database path.

This functionality is essential for cross-database operations such as database creation from templates, database copying, or administrative tasks that need to examine relations in other databases without switching the current database context.

The function directly reads the relmap file from the target database directory and performs a linear search through the mappings to find the requested relation OID.

## Parameters / Member Variables
- `dbpath`: String path to the database directory containing the relation mapping file to read
- `relationId`: The OID of the relation whose file number is being sought

## Dependencies
- Functions called/Symbols referenced:
  - [RelMapFile](RelMapFile.md) (structure type for holding mapping data)
  - [read_relmap_file](../r/read_relmap_file.md) (function to read mapping file from disk)
  - InvalidRelFileNumber (returned when no mapping is found)
- Called from (representative examples):
  - [ScanSourceDatabasePgClass](../S/ScanSourceDatabasePgClass.md) (dbcommands.c:265)
  - [ScanSourceDatabasePgClassTuple](../S/ScanSourceDatabasePgClassTuple.md) (dbcommands.c:423)

## Notes and Other Information
- Does not consider active updates or pending transactions since it reads directly from the persistent mapping file
- Used primarily during database administration operations like CREATE DATABASE FROM TEMPLATE
- Returns InvalidRelFileNumber when the relation OID is not found in the target database's mapping
- The function assumes the target database's relmap file is accessible and valid
- Error handling is delegated to the read_relmap_file function with ERROR level reporting