# GetCommandTagNameAndLen

## Location
[src/backend/tcop/cmdtag.c:53-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/cmdtag.c#L53-L59)

## Overview
Returns both the textual name string and its length for a given CommandTag enumeration value in a single function call.

## Definition

```c
const char *
GetCommandTagNameAndLen(CommandTag commandTag, Size *len)
```
## Detailed Description
This function is an optimized variant of GetCommandTagName that also retrieves the pre-computed length of the command tag name string. It performs a single lookup into the tag_behavior array to fetch both the name string and its length, avoiding the need for a separate strlen() call when both pieces of information are needed.

The function sets the output parameter *len to the namelen field from the CommandTagBehavior structure, which contains the pre-calculated string length (computed as sizeof(name) - 1 during initialization). This optimization is particularly useful in performance-critical code paths where string length information is frequently needed alongside the string itself.

## Parameters / Member Variables
- : The CommandTag enumeration value for which to retrieve the name and length
- : Output parameter that receives the length of the returned string (in bytes, excluding null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - CommandTag (enum type)
  - Size (typedef for size_t)
  - tag_behavior (static array of CommandTagBehavior structs)
- Called from (representative examples):
  - [BuildQueryCompletionString](../B/BuildQueryCompletionString.md) (src/backend/tcop/cmdtag.c:126)
  - [exec_simple_query](../e/exec_simple_query.md) (src/backend/tcop/postgres.c:1124)
  - [exec_execute_message](../e/exec_execute_message.md) (src/backend/tcop/postgres.c:2178)
  - [CopyQueryCompletion](../C/CopyQueryCompletion.md) (src/include/tcop/cmdtag.h:54)

## Notes and Other Information
- The returned length excludes the null terminator (consistent with strlen() behavior)
- The namelen field in CommandTagBehavior is computed at compile time using sizeof(name) - 1
- This function provides better performance than calling GetCommandTagName() followed by strlen()
- Used primarily in query completion string building and protocol message formatting where both name and length are required
- The len parameter must not be NULL, as the function unconditionally dereferences it

## Simplified Source

```c
// Simplified version of GetCommandTagNameAndLen
const char *
GetCommandTagNameAndLen(CommandTag commandTag, Size *len)
{
    // Look up the command tag entry in the behavior table
    // and extract both name and pre-computed length
    *len = tag_behavior[commandTag].namelen;
    return tag_behavior[commandTag].name;
}
```

Key simplifications made:
- Added explanatory comments for the core logic
- The function is already quite simple - it performs a single array lookup
- No simplification was needed as the original is already minimal and efficient