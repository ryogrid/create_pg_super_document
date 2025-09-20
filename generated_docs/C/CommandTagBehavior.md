# CommandTagBehavior

## Location
[src/backend/tcop/cmdtag.c:20-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/cmdtag.c#L20-L28)

## Overview
CommandTagBehavior is a structure that defines the metadata and behavioral properties for each SQL command tag in PostgreSQL's command completion system.

## Definition

```c
typedef struct CommandTagBehavior
{
	const char *name;			/* tag name, e.g. "SELECT" */
	const uint8 namelen;		/* set to strlen(name) */
	const bool	event_trigger_ok;
	const bool	table_rewrite_ok;
	const bool	display_rowcount;	/* should the number of rows affected be
									 * shown in the command completion string */
} CommandTagBehavior;
```
## Detailed Description
CommandTagBehavior serves as a lookup table entry that stores the characteristics and behavior rules for each SQL command tag in PostgreSQL. This structure is used to create the  array in src/backend/tcop/cmdtag.c:33, which provides a centralized repository of command tag metadata. Each entry corresponds to a CommandTag enum value and defines how that command should behave in various contexts, particularly for command completion strings, event triggers, and table rewrite operations.

The structure is populated through the PG_CMDTAG macro expansion from the cmdtaglist.h file, ensuring consistency between the CommandTag enumeration and the corresponding behavior definitions. This design allows PostgreSQL to efficiently look up command properties without needing separate switch statements or conditional logic scattered throughout the codebase.

## Parameters / Member Variables
- : String containing the human-readable command tag name (e.g., "SELECT", "INSERT", "UPDATE")
- : Pre-calculated length of the name string for performance optimization
- : Boolean flag indicating whether this command can trigger event triggers
- : Boolean flag indicating whether this command can perform table rewrites
- : Boolean flag determining if the number of affected rows should be included in the command completion string

## Dependencies
- Functions called/Symbols referenced: 
  - None (this is a data structure definition)
- Called from (representative examples):
  - [GetCommandTagName](../G/GetCommandTagName.md) (src/backend/tcop/cmdtag.c:48)
  - [GetCommandTagNameAndLen](../G/GetCommandTagNameAndLen.md) (src/backend/tcop/cmdtag.c:54)
  - [command_tag_display_rowcount](../c/command_tag_display_rowcount.md) (src/backend/tcop/cmdtag.c:61)
  - [command_tag_event_trigger_ok](../c/command_tag_event_trigger_ok.md) (src/backend/tcop/cmdtag.c:67)
  - [command_tag_table_rewrite_ok](../c/command_tag_table_rewrite_ok.md) (src/backend/tcop/cmdtag.c:73)
  - [GetCommandTagEnum](../G/GetCommandTagEnum.md) (src/backend/tcop/cmdtag.c:85)
  - [BuildQueryCompletionString](../B/BuildQueryCompletionString.md) (src/backend/tcop/cmdtag.c:124)

## Notes and Other Information
- The structure is used exclusively as a const data structure in the tag_behavior array
- The PG_CMDTAG macro at src/backend/tcop/cmdtag.c:30 is used to initialize instances of this structure
- The namelen field is computed at compile time using sizeof(name)-1 for efficiency
- This structure is central to PostgreSQL's command completion protocol and affects what clients see in response to SQL commands
- The structure supports PostgreSQL's wire protocol compatibility requirements, particularly for command completion tags
- Event triggers and table rewrite permissions are controlled through the boolean flags in this structure