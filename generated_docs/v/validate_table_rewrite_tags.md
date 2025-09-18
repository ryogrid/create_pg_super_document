# validate_table_rewrite_tags

## Location
src/backend/commands/event_trigger.c: 239 - 260

## Overview
Validates DDL command tags for table_rewrite event triggers, ensuring only commands that can cause table rewrites are allowed as filter conditions.

## Definition
```c
static void validate_table_rewrite_tags(const char *filtervar, List *taglist)
```

## Detailed Description
validate_table_rewrite_tags is a static helper function specifically designed to validate command tags for table_rewrite event triggers. Unlike validate_ddl_tags which validates general DDL commands, this function focuses on commands that can trigger table rewrites (such as ALTER TABLE operations that change column types or add columns with defaults). It uses command_tag_table_rewrite_ok() to determine if each command tag is appropriate for table rewrite events, ensuring that only relevant commands can be filtered in table_rewrite event triggers.

## Parameters / Member Variables
- `filtervar`: The name of the filter variable (typically "tag") used in error messages for context
- `taglist`: A List containing string values representing DDL command tags to validate for table rewrite events

## Dependencies
- Functions called/Symbols referenced:
  - strVal() - extracts string value from a Node
  - [GetCommandTagEnum](../G/GetCommandTagEnum.md)() - converts command tag string to CommandTag enum
  - [command_tag_table_rewrite_ok](../c/command_tag_table_rewrite_ok.md)() - checks if command tag is valid for table rewrite events
  - ereport() - reports errors with appropriate error codes
- Called from (representative examples):
  - [CreateEventTrigger](../C/CreateEventTrigger.md)() - when creating event triggers with table_rewrite event and tag filters

## Notes and Other Information
- This is a static function only accessible within event_trigger.c
- Specifically validates tags for table_rewrite events, which fire when table structure changes require rewriting table data
- Reports ERRCODE_FEATURE_NOT_SUPPORTED for commands that don't support table rewrite event triggers
- More restrictive than validate_ddl_tags as it only allows commands that can actually cause table rewrites
- Part of the specialized validation for table_rewrite event triggers introduced to help track table restructuring operations
- Helps database administrators and tools monitor when tables are being physically rewritten, which can be expensive operations