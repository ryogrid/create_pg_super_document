# is_ancestor_member_tableinfos

## Location
[src/backend/catalog/pg_publication.c:182-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L182-L200)

## Overview
A static helper function that checks whether a given ancestor table OID exists in a list of published relation information structures.

## Definition


## Detailed Description
This function iterates through a list of  structures to determine if a specified ancestor table (identified by its OID) is present in the publication's table information list. It performs a simple linear search through the list, comparing each relation's OID with the target ancestor OID. The function is used internally within PostgreSQL's publication system to check table membership during partition filtering operations.

## Parameters / Member Variables
- : The OID of the ancestor table to search for in the publication list
- : A List containing  structures representing tables included in a publication

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type for publication relation information)
  -  (List iteration macro)
  -  (List iteration macro)
- Called from (representative examples):
  -  (src/backend/catalog/pg_publication.c:219)

## Notes and Other Information
This is a static function used internally within pg_publication.c for publication management. It's specifically designed to support the partition filtering logic where it's necessary to check if ancestor tables are explicitly included in a publication before making decisions about partition inclusion. The function uses PostgreSQL's standard List iteration patterns and returns immediately upon finding a match for efficiency.