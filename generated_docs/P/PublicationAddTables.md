# PublicationAddTables

## Location
src/backend/commands/publicationcmds.c: 1747 - 1780

## Overview
Adds a list of tables to an existing publication with proper permission checks and event trigger support.

## Definition


## Detailed Description
PublicationAddTables is a static function that handles the addition of multiple tables to a publication. The function performs comprehensive validation and integration:

1. **Permission Validation**: Verifies that the current user is either the table owner or a superuser using object_ownercheck()
2. **Publication Integration**: Calls publication_add_relation() to establish the relationship between the publication and each table in the catalog
3. **Event System Integration**: When an AlterPublicationStmt is provided, collects the command for event triggers and invokes post-create hooks for proper system integration
4. **Error Handling**: Provides appropriate access control error messages when permission checks fail

The function ensures that only authorized users can add tables to publications and maintains proper integration with PostgreSQL's event trigger and dependency tracking systems.

## Parameters / Member Variables
- : OID of the publication to which tables will be added
- : List of PublicationRelInfo structures containing the tables and their associated metadata (WHERE clauses, column lists)
- : Boolean flag indicating whether to suppress errors if the table is already in the publication
- : Optional AlterPublicationStmt for event trigger integration (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [object_ownercheck](../o/object_ownercheck.md) (validates table ownership)
  - [publication_add_relation](../p/publication_add_relation.md) (adds relation to publication catalog)
  - [aclcheck_error](../a/aclcheck_error.md) (reports access control errors)
  - [get_relkind_objtype](../g/get_relkind_objtype.md) (gets object type for error messages)
  - [EventTriggerCollectSimpleCommand](../E/EventTriggerCollectSimpleCommand.md) (event trigger integration)
  - InvokeObjectPostCreateHook (post-creation hook invocation)
- Called from (representative examples):
  - [CreatePublication](../C/CreatePublication.md) (src/backend/commands/publicationcmds.c:839)
  - [AlterPublicationTables](../A/AlterPublicationTables.md) (src/backend/commands/publicationcmds.c:1106)
  - [AlterPublicationTables](../A/AlterPublicationTables.md) (src/backend/commands/publicationcmds.c:1235)

## Notes and Other Information
- Includes assertion to ensure the statement is not for 'FOR ALL TABLES' publications
- Proper integration with PostgreSQL's event trigger system for DDL tracking
- Uses PublicationRelRelationId for post-create hook invocation
- Access control follows PostgreSQL's standard ownership model (owner or superuser)
- Supports conditional addition through if_not_exists parameter to handle duplicate table scenarios gracefully