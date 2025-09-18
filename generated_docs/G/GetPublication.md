# GetPublication

## Location
src/backend/catalog/pg_publication.c: 1006 - 1036

## Overview
Retrieves and constructs a Publication structure by looking up publication information from the system catalog using a publication OID.

## Definition
Publication *GetPublication(Oid pubid)

## Detailed Description
This function performs a system catalog lookup to retrieve publication metadata and constructs a complete Publication structure containing all relevant publication properties. It searches the pg_publication system catalog using the provided publication OID and extracts information such as publication name, table inclusion options, and various publication actions (insert, update, delete, truncate operations).

The function allocates memory for the Publication structure and its string components, making a complete copy of all publication data. It handles the conversion from the system catalog tuple format to the internal Publication structure used throughout the PostgreSQL logical replication system.

## Parameters / Member Variables
- : The OID of the publication to retrieve from the system catalog

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1: Searches the system cache for publication by OID
  - HeapTupleIsValid: Validates the retrieved heap tuple
  - GETSTRUCT: Macro to extract structure from heap tuple
  - palloc: Allocates memory for the Publication structure
  - pstrdup: Duplicates the publication name string
  - NameStr: Extracts string from Name type
  - ReleaseSysCache: Releases the system cache tuple
- Called from (representative examples):
  - publication_add_relation: Adds relations to existing publications
  - publication_add_schema: Adds schemas to existing publications
  - GetPublicationByName: Used as helper for name-based lookup
  - NUM_PUBLICATION_TABLES_ELEM: Counts tables in publication

## Notes and Other Information
- Throws an ERROR if the publication OID is not found in the system catalog
- The returned Publication structure is allocated with palloc and must be freed by the caller
- Copies all publication action flags (pubinsert, pubupdate, pubdelete, pubtruncate)
- Includes the pubviaroot flag that controls partition publication behavior
- Essential building block for all publication-related operations in logical replication
- The Publication structure contains both metadata and behavioral configuration for the publication