# command_tag_display_rowcount

## Location
[src/backend/tcop/cmdtag.c:60-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/cmdtag.c#L60-L65)

## Overview
Determines whether a given CommandTag should include the number of affected rows in its completion message.

## Definition

```c
bool
command_tag_display_rowcount(CommandTag commandTag)
```
## Detailed Description
This function checks the display_rowcount property of a command tag to determine whether the command completion string should include information about the number of rows processed by the command. Commands like SELECT, INSERT, UPDATE, DELETE, COPY, FETCH, MOVE, and MERGE typically display row counts in their completion messages (e.g., "SELECT 5", "UPDATE 10"), while DDL commands like CREATE TABLE or ALTER INDEX do not.

The function performs a direct lookup into the tag_behavior array to retrieve the pre-configured display_rowcount boolean flag for the specified command tag. This flag is set during compile-time initialization based on the data in cmdtaglist.h.

## Parameters / Member Variables
- `commandTag`: The CommandTag enumeration value to check for row count display behavior
## Dependencies
- Functions called/Symbols referenced:
  - CommandTag (enum type)
  - tag_behavior (static array of CommandTagBehavior structs)
- Called from (representative examples):
  - [BuildQueryCompletionString](../B/BuildQueryCompletionString.md) (src/backend/tcop/cmdtag.c:146)
  - [CopyQueryCompletion](../C/CopyQueryCompletion.md) (src/include/tcop/cmdtag.h:55)

## Notes and Other Information
- Returns true for DML commands that typically process multiple rows: SELECT, INSERT, UPDATE, DELETE, COPY, FETCH, MOVE, MERGE
- Returns false for most DDL commands and utility commands that don't have meaningful row counts
- Used primarily in BuildQueryCompletionString to determine whether to append row count information to completion tags
- The display_rowcount flag is the fifth parameter in the PG_CMDTAG macro definitions in cmdtaglist.h
- This design allows PostgreSQL to provide consistent and meaningful completion messages to clients across different command types

## Simplified Source

```c
// Simplified version of command_tag_display_rowcount
bool command_tag_display_rowcount(CommandTag commandTag) {
    // Direct lookup of display behavior from command tag table
    return tag_behavior[commandTag].display_rowcount;
}
```

Key simplifications made:
- Preserved core table lookup functionality
- Added comment explaining the purpose
- Maintained simple, direct access pattern