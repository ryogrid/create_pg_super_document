# PublicationActions

## Location
src/include/catalog/pg_publication.h: 72 - 78

## Overview
PublicationActions is a structure that defines which DML operations (Data Manipulation Language) are published by a PostgreSQL logical replication publication.

## Definition


## Detailed Description
PublicationActions is a simple structure that serves as a configuration container for logical replication publications. It controls which types of DML operations on published tables should be replicated to subscribers. Each boolean field corresponds to a specific SQL operation type, allowing fine-grained control over what changes are captured and transmitted during logical replication.

This structure is fundamental to PostgreSQL's logical replication system, as it determines the scope of data changes that will be sent from publishers to subscribers. The structure is used throughout the publication management system to store, validate, and apply publication operation filters.

## Parameters / Member Variables
- : Boolean flag indicating whether INSERT operations should be published
- : Boolean flag indicating whether UPDATE operations should be published  
- : Boolean flag indicating whether DELETE operations should be published
- : Boolean flag indicating whether TRUNCATE operations should be published

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)
- Called from (representative examples):
  - [parse_publication_options](../p/parse_publication_options.md) (src/backend/commands/publicationcmds.c:79)
  - [CreatePublication](../C/CreatePublication.md) (src/backend/commands/publicationcmds.c:737)
  - [AlterPublicationOptions](../A/AlterPublicationOptions.md) (src/backend/commands/publicationcmds.c:878)
  - [RelationSyncEntry](../R/RelationSyncEntry.md) (src/backend/replication/pgoutput/pgoutput.c:143)
  - [PublicationDesc](PublicationDesc.md) (src/include/catalog/pg_publication.h:82)
  - [Publication](Publication.md) (src/include/catalog/pg_publication.h:106)

## Notes and Other Information
- This structure is defined in the catalog header file, indicating its role as a fundamental data type for the publication system
- All four operation types (INSERT, UPDATE, DELETE, TRUNCATE) can be independently controlled
- The structure is used both for storing publication configuration and for runtime decision-making during replication
- Default values and validation logic for these flags are handled by the functions that use this structure