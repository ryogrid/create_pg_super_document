# cbrdissect

## Location
[src/backend/regex/regexec.c:994-1075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L994-L1075)

## Overview
Implements backref (backreference) node dissection in regular expression matching by verifying that the target substring matches a specified number of repetitions of a previously captured group.

## Definition

```c
static int						/* regexec return code */
cbrdissect(struct vars *v,
		   struct subre *t,
		   chr *begin,			/* beginning of relevant substring */
		   chr *end)			/* end of same */
```
## Detailed Description
The  function handles backreference nodes in regular expression matching. A backreference refers to a previously captured group and requires that the current position in the string matches exactly the same text that was captured by that group, repeated a specified number of times.

The function performs several validation steps:
1. Retrieves the backreferenced string from the captured groups array
2. Handles special cases for zero-length strings and zero-length targets
3. Validates that the target length could represent an allowed number of repetitions
4. Compares the actual string contents repetition by repetition

The algorithm is optimized for the common cases where either the backreference or target is zero-length, and includes detailed validation of repetition counts against the specified minimum and maximum bounds.

## Parameters
- : Pointer to the vars struct containing regex execution context, captured groups, and comparison functions
- : Pointer to the subre (subexpression) struct representing the backref node with backno, min, and max fields
- : Pointer to the beginning character of the substring to match against the backreference
- : Pointer to the end character of the substring to match

## Dependencies
- Functions called/Symbols referenced:
  - : String comparison function from the regex grammar context
  - : Macro for debug output
  - : Macro for converting pointers to offsets
  - : Return code for failed matches
  - : Return code for successful matches
  - : Constant representing infinite repetitions
- Called from:
  - : Main dissection dispatcher function

## Notes and Other Information
- The function validates that the backreference group was actually captured (rm_so != -1)
- Handles edge cases where either the backreference or target string has zero length
- Performs modular arithmetic to ensure target length is compatible with repetition requirements
- Uses the grammar's comparison function to handle case-insensitive matching when appropriate
- Includes bounds checking for minimum and maximum repetition counts
- The backref node operation is identified by op == 'b' and contains a backno field indicating which captured group to reference