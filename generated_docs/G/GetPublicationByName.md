# GetPublicationByName

## Location
[src/backend/catalog/pg_publication.c:1037-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L1037-L1051)

## Overview
Retrieves a Publication structure by looking up a publication using its name, with optional handling for missing publications.

## Definition
Publication *GetPublicationByName(const char *pubname, bool missing_ok)

## Detailed Description
This function provides a name-based interface for retrieving publication information from the PostgreSQL system catalog. It acts as a wrapper around the OID-based GetPublication function by first resolving the publication name to its corresponding OID, then delegating to GetPublication for the actual data retrieval. The function supports optional error handling through the missing_ok parameter, allowing callers to choose between error reporting or NULL return for non-existent publications.

This is a common pattern in PostgreSQL where both name-based and OID-based access methods are provided for system catalog objects, with the name-based version serving as a convenience wrapper for user-facing operations.

## Parameters / Member Variables
- : The name of the publication to retrieve (null-terminated string)
- : Boolean flag controlling error behavior - if true, returns NULL for missing publications; if false, raises an error

## Dependencies
- Functions called/Symbols referenced:
  - [get_publication_oid](../g/get_publication_oid.md): Resolves publication name to OID, with error handling controlled by missing_ok
  - [GetPublication](GetPublication.md): Retrieves the full Publication structure using the resolved OID
  - OidIsValid: Validates the OID returned by get_publication_oid
- Called from (representative examples):
  - [get_object_address_publication_rel](../g/get_object_address_publication_rel.md): Object address resolution for publication relations
  - [get_object_address_publication_schema](../g/get_object_address_publication_schema.md): Object address resolution for publication schemas  
  - NUM_PUBLICATION_TABLES_ELEM: Counts tables when publication is specified by name
  - [LoadPublications](../L/LoadPublications.md): Loads publication configurations in pgoutput plugin

## Notes and Other Information
- Returns NULL if the publication doesn't exist and missing_ok is true
- Raises an error if the publication doesn't exist and missing_ok is false
- The returned Publication structure is allocated with palloc and must be freed by the caller
- Commonly used in DDL commands and replication setup where publications are specified by name
- Part of the standard PostgreSQL pattern of providing both name-based and OID-based catalog access functions