# PublicationDropTables

## Location
src/backend/commands/publicationcmds.c: 1781 - 1825

## Overview
Removes a list of tables from an existing publication with validation to ensure proper syntax and existence checks.

## Definition


## Detailed Description
PublicationDropTables is a static function that handles the removal of tables from a publication. The function performs several validation steps and cleanup operations:

1. **Syntax Validation**: Ensures that column lists and WHERE clauses are not specified in DROP operations, as these are only valid for ADD operations
2. **Existence Verification**: Checks if each table is actually part of the publication by looking up the publication-relation mapping in pg_publication_rel catalog
3. **Graceful Error Handling**: Supports optional missing_ok parameter to allow silent continuation when tables are not found in the publication
4. **Catalog Cleanup**: Uses performDeletion() with DROP_CASCADE to properly remove the publication-relation relationship and handle any dependent objects

The function ensures data integrity by validating that only valid operations are performed and that the catalog remains consistent after table removal.

## Parameters / Member Variables
- : OID of the publication from which tables will be removed
- : List of PublicationRelInfo structures containing the tables to be removed
- : Boolean flag indicating whether to silently ignore tables that are not part of the publication

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid2 (looks up publication-relation mapping)
  - ObjectAddressSet (sets up object address for deletion)
  - performDeletion (performs catalog deletion with cascade)
  - RelationGetRelid (gets relation OID)
  - RelationGetRelationName (gets relation name for error messages)
- Called from (representative examples):
  - AlterPublicationTables (src/backend/commands/publicationcmds.c:1109)
  - AlterPublicationTables (src/backend/commands/publicationcmds.c:1229)

## Notes and Other Information
- Strict validation prevents column lists and WHERE clauses in DROP operations to maintain clear syntax semantics
- Uses DROP_CASCADE for deletion to ensure proper cleanup of dependent catalog entries
- Leverages pg_publication_rel catalog (PUBLICATIONRELMAP) to verify table membership
- Error handling distinguishes between syntax errors and missing table scenarios
- Essential for maintaining publication consistency when tables are removed from logical replication setup