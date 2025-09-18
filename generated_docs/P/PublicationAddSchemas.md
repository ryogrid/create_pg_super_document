# PublicationAddSchemas

## Location
src/backend/commands/publicationcmds.c: 1826 - 1853

## Overview
Adds a list of schemas to an existing PostgreSQL publication, establishing publication-namespace relationships for logical replication.

## Definition


## Detailed Description
This static function iterates through a list of schema OIDs and adds each schema to the specified publication by creating publication-namespace mappings. It serves as a higher-level wrapper around the  function, handling batch operations and event trigger notifications. The function ensures that schemas are properly associated with publications for logical replication purposes, and optionally handles duplicate schema additions gracefully with the  parameter.

The function also manages event trigger collection and post-creation hooks when called in the context of an ALTER PUBLICATION statement, ensuring proper event handling and notification for schema additions.

## Parameters / Member Variables
- : OID of the target publication to which schemas will be added
- : List of schema OIDs to be added to the publication
- : Boolean flag to control behavior when a schema already exists in the publication (true = skip duplicates, false = raise error)
- : Pointer to AlterPublicationStmt structure for event trigger context; can be NULL if not called from ALTER PUBLICATION

## Dependencies
- Functions called/Symbols referenced:
  - publication_add_schema
  - EventTriggerCollectSimpleCommand
  - InvokeObjectPostCreateHook
  - AlterPublicationStmt (structure type)
- Called from (representative examples):
  - CreatePublication
  - AlterPublicationSchemas

## Notes and Other Information
- This is a static function, only accessible within the publicationcmds.c compilation unit
- Contains an assertion that ensures stmt is NULL or stmt->for_all_tables is false, preventing conflicts with FOR ALL TABLES publications
- Event trigger collection and post-creation hooks are only invoked when stmt parameter is not NULL
- Each schema addition creates an ObjectAddress that is used for event trigger notifications
- The function operates on publication-namespace relationships stored in the pg_publication_namespace system catalog