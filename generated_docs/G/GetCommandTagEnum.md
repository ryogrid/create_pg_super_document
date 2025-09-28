# GetCommandTagEnum

## Location
[src/backend/tcop/cmdtag.c:83-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/cmdtag.c#L83-L120)

## Overview
Performs a binary search to convert a command name string into its corresponding CommandTag enumeration value, returning CMDTAG_UNKNOWN if the command is not recognized.

## Definition

```c
CommandTag
GetCommandTagEnum(const char *commandname)
```
## Detailed Description
This function implements a binary search algorithm to efficiently locate a command tag by its string name within the sorted  array. It performs case-insensitive string comparison using  to match the input command name against the stored command tag names. The function is designed for high performance lookups, utilizing the fact that command tag names are stored in sorted order.

The binary search implementation follows the standard algorithm: it starts with the full array range, compares the target string with the middle element, and narrows the search range based on the comparison result. This provides O(log n) lookup performance.

If the input is NULL or an empty string, the function immediately returns CMDTAG_UNKNOWN without performing any search.

## Parameters / Member Variables
- : A null-terminated string containing the command name to look up (e.g., "SELECT", "INSERT", "CREATE TABLE")

## Dependencies
- Functions called/Symbols referenced:
  -  (case-insensitive string comparison)
  -  (macro to get array length)
  -  (global array of CommandTagBehavior structures)
  - CommandTag (enum type)
  - [CommandTagBehavior](../C/CommandTagBehavior.md) (struct type)
- Called from (representative examples):
  -  (src/backend/commands/event_trigger.c:219)
  -  (src/backend/commands/event_trigger.c:246)
  -  (src/backend/utils/cache/evtcache.c:238)
  -  (src/include/tcop/cmdtag.h:58)

## Notes and Other Information
- Returns CMDTAG_UNKNOWN for unrecognized commands, NULL input, or empty strings
- Uses binary search for O(log n) performance, requiring the tag_behavior array to be sorted by command name
- The case-insensitive comparison allows for flexible command name matching
- Critical function for event trigger validation and command tag resolution throughout PostgreSQL
- The returned CommandTag can be used as an index into the tag_behavior array for accessing command properties

## Simplified Source

```c
// Simplified version of GetCommandTagEnum
CommandTag GetCommandTagEnum(const char *commandname) {
    const CommandTagBehavior *base, *last, *position;
    int result;

    // Handle null or empty input
    if (commandname == NULL || *commandname == '\0') {
        return CMDTAG_UNKNOWN;
    }

    // Binary search through sorted tag_behavior array
    base = tag_behavior;
    last = tag_behavior + lengthof(tag_behavior) - 1;

    while (last >= base) {
        position = base + ((last - base) >> 1);
        result = pg_strcasecmp(commandname, position->name);

        if (result == 0) {
            // Found match - return CommandTag index
            return (CommandTag)(position - tag_behavior);
        } else if (result < 0) {
            last = position - 1;
        } else {
            base = position + 1;
        }
    }

    return CMDTAG_UNKNOWN;
}
```

Key simplifications made:
- Preserved complete binary search algorithm
- Maintained case-insensitive string comparison
- Kept essential null/empty input validation
- Focused on core lookup functionality