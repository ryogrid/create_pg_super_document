# command_tag_event_trigger_ok

## Location
src/backend/tcop/cmdtag.c: 66 - 71

## Overview
Determines whether a given CommandTag is eligible to fire event triggers during its execution.

## Definition


## Detailed Description
This function checks the event_trigger_ok property of a command tag to determine whether the command is permitted to fire event triggers. Event triggers are special triggers that fire on DDL commands and certain utility commands, providing hooks for extensions and custom logic to execute before or after schema changes.

The function performs a direct lookup into the tag_behavior array to retrieve the pre-configured event_trigger_ok boolean flag. Commands that are eligible for event triggers are typically DDL commands (CREATE, ALTER, DROP) and some utility commands, while transactional commands (BEGIN, COMMIT, ROLLBACK), system commands (ALTER SYSTEM), and commands that operate outside normal transactional context generally are not eligible.

## Parameters / Member Variables
- : The CommandTag enumeration value to check for event trigger eligibility

## Dependencies
- Functions called/Symbols referenced:
  - CommandTag (enum type)
  - tag_behavior (static array of CommandTagBehavior structs)
- Called from (representative examples):
  - validate_ddl_tags (src/backend/commands/event_trigger.c:226)
  - EventTriggerCommonSetup (src/backend/commands/event_trigger.c:668)
  - CopyQueryCompletion (src/include/tcop/cmdtag.h:56)

## Notes and Other Information
- Returns true for most DDL commands like CREATE TABLE, ALTER FUNCTION, DROP INDEX, etc.
- Returns false for transaction control commands (BEGIN, COMMIT, ROLLBACK), system administration commands (ALTER SYSTEM), and database/role management commands
- Used by the event trigger system to validate which commands can have event triggers defined for them
- The event_trigger_ok flag is the third parameter in the PG_CMDTAG macro definitions in cmdtaglist.h
- Event triggers provide extensibility points for database administrators and extension developers to implement custom logic around schema changes
- This design ensures event triggers only fire on appropriate commands where they make semantic sense