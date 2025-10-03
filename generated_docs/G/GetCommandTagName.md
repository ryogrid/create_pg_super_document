# GetCommandTagName

## Location
[src/backend/tcop/cmdtag.c:47-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/cmdtag.c#L47-L52)

## Overview
Returns the textual name string associated with a given CommandTag enumeration value.

## Definition

```c
const char *
GetCommandTagName(CommandTag commandTag)
```
## Detailed Description
This function provides a simple lookup mechanism to retrieve the human-readable string representation of a PostgreSQL command tag. It accesses the static tag_behavior array, which contains CommandTagBehavior structures that map each CommandTag enum value to its corresponding textual name (e.g., "SELECT", "INSERT", "CREATE TABLE", etc.).

The function performs a direct array lookup using the commandTag as an index into the tag_behavior array, making it a very efficient O(1) operation. The returned string is a constant literal that should not be modified by the caller.

## Parameters / Member Variables
- `commandTag`: The CommandTag enumeration value for which to retrieve the name string
## Dependencies
- Functions called/Symbols referenced:
  - CommandTag (enum type)
  - tag_behavior (static array of CommandTagBehavior structs)
- Called from (representative examples):
  - [EventTriggerCommonSetup](../E/EventTriggerCommonSetup.md) (src/backend/commands/event_trigger.c:669, 674)
  - [interpret_AS_clause](../i/interpret_AS_clause.md) (src/backend/commands/functioncmds.c:940, 959)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md) (src/backend/executor/spi.c:1608)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md) (src/backend/executor/spi.c:2566)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (src/backend/tcop/utility.c:578, 580, 582)
  - [CopyQueryCompletion](../C/CopyQueryCompletion.md) (src/include/tcop/cmdtag.h:53)

## Notes and Other Information
- The tag_behavior array is initialized using the PG_CMDTAG macro with data from cmdtaglist.h
- Each CommandTagBehavior entry contains the name string, its length, and boolean flags for various properties
- The function assumes the commandTag parameter is a valid index within the tag_behavior array bounds
- Returns a pointer to a constant string that remains valid for the lifetime of the program
- Used extensively throughout PostgreSQL for logging, error messages, completion tags, and event trigger processing

## Simplified Source

```c
// Simplified version of GetCommandTagName
const char *GetCommandTagName(CommandTag commandTag) {
    // Direct lookup into behavior table to get command name string
    return tag_behavior[commandTag].name;
}
```

Key simplifications made:
- Function is already very simple, only added clarifying comment
- Core logic remains unchanged as it's a straightforward table lookup
- Preserved the constant string return type