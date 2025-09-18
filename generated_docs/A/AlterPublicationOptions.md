# AlterPublicationOptions

## Location
src/backend/commands/publicationcmds.c: 871 - 1057

## Overview
AlterPublicationOptions modifies the options of an existing publication, handling changes to publish actions and partition root publishing preferences while enforcing constraints related to WHERE clauses and column lists.

## Definition


## Detailed Description
AlterPublicationOptions is a static function that handles the modification of publication options such as publish actions (insert, update, delete, truncate) and the publish_via_partition_root setting. The function performs comprehensive validation to ensure that certain combinations of settings are not allowed, particularly when disabling publish_via_partition_root for publications containing partitioned tables with WHERE clauses or column lists.

The function parses the new options, validates constraints (especially for partitioned tables), updates the catalog tuple, and invalidates the appropriate relation cache entries. It includes sophisticated logic to handle partition hierarchies and ensures consistency between publication options and existing table configurations.

## Parameters / Member Variables
- : ParseState containing parsing context and source text information
- : AlterPublicationStmt structure containing the alteration command details
- : Relation object for the pg_publication catalog table
- : HeapTuple representing the existing publication record to be modified

## Dependencies
- Functions called/Symbols referenced:
  - parse_publication_options: Parses publication-specific options from the statement
  - LockDatabaseObject: Locks the publication to prevent concurrent modifications
  - GetPublicationRelations: Retrieves relations associated with the publication
  - heap_attisnull: Checks for NULL values in tuple attributes (for WHERE clauses and column lists)
  - get_rel_relkind/get_rel_name: Retrieves relation metadata for validation
  - heap_modify_tuple: Creates a modified version of the publication tuple
  - CatalogTupleUpdate: Updates the publication record in the catalog
  - InvalidatePublicationRels: Invalidates relation cache entries for affected tables
  - GetAllSchemaPublicationRelations: Gets schema-based publication relations
- Called from (representative examples):
  - AlterPublication: Main function handling publication alterations

## Notes and Other Information
- Enforces the constraint that partitioned tables with WHERE clauses or column lists cannot exist when publish_via_partition_root is false
- Handles both explicit table publications and schema-based publications
- Performs sophisticated partition tree traversal to invalidate all affected relations
- Uses system cache lookups to validate existing publication-relation mappings
- Includes comprehensive error reporting for constraint violations
- Supports event trigger integration for DDL command tracking
- Handles concurrent table drops gracefully by checking for NULL relation names