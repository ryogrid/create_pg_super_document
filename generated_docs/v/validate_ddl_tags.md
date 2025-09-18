# validate_ddl_tags

## Location
src/backend/commands/event_trigger.c: 212 - 238

## Overview
Validates DDL command tags specified in event trigger filter conditions, ensuring they are recognized commands and supported by the event trigger system.

## Definition
```c
static void validate_ddl_tags(const char *filtervar, List *taglist)
```

## Detailed Description
validate_ddl_tags is a static helper function that validates a list of DDL command tags provided as filter conditions for event triggers. It iterates through each tag in the list, converts the tag string to a CommandTag enum using GetCommandTagEnum(), and verifies that the command is both recognized and supported by the event trigger system. This validation prevents invalid or unsupported commands from being used in event trigger definitions.

## Parameters / Member Variables
- `filtervar`: The name of the filter variable (typically "tag") used in error messages to provide context
- `taglist`: A List containing string values representing DDL command tags to validate

## Dependencies
- Functions called/Symbols referenced:
  - strVal() - extracts string value from a Node
  - GetCommandTagEnum() - converts command tag string to CommandTag enum
  - command_tag_event_trigger_ok() - checks if command tag is supported by event triggers
  - ereport() - reports errors with appropriate error codes
- Called from (representative examples):
  - CreateEventTrigger() - when creating event triggers with DDL tag filters

## Notes and Other Information
- This is a static function only accessible within event_trigger.c
- Validates tags for ddl_command_start, ddl_command_end, and sql_drop events
- Reports ERRCODE_SYNTAX_ERROR for unrecognized command tags
- Reports ERRCODE_FEATURE_NOT_SUPPORTED for commands that don't support event triggers
- Part of the event trigger validation pipeline that ensures only valid and supported DDL commands can be filtered
- The function helps maintain the integrity of the event trigger system by preventing registration of triggers for unsupported operations