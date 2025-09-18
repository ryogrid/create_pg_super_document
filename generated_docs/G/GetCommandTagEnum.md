# GetCommandTagEnum

## Location
src/backend/tcop/cmdtag.c: 83 - 120

## Overview
Performs a binary search to convert a command name string into its corresponding CommandTag enumeration value, returning CMDTAG_UNKNOWN if the command is not recognized.

## Definition


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
  - CommandTagBehavior (struct type)
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