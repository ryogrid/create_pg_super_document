# crevcondissect

## Location
[src/backend/regex/regexec.c:910-993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L910-L993)

## Overview
Implements the dissection algorithm for concatenation nodes in regular expression matching using a shortest-first strategy for the left operand.

## Definition

```c
static int						/* regexec return code */
crevcondissect(struct vars *v,
			   struct subre *t,
			   chr *begin,		/* beginning of relevant substring */
			   chr *end)		/* end of same */
```
## Detailed Description
The  function is a specialized dissection function for concatenation operators ('.') in regular expression parsing. It employs a shortest-first approach where the left child of the concatenation is matched with the shortest possible substring first, then the right child is tested with the remaining portion. This is the reverse strategy compared to , which uses a longest-first approach for the left operand.

The function works by iteratively finding tentative midpoints using the  function for the left operand, then verifying if the right operand can match from that midpoint to the end. If both sides match successfully, the function returns success. If not, it advances to the next possible shortest match for the left side and repeats the process.

## Parameters
- : Pointer to the vars struct containing regex execution context and state information
- : Pointer to the subre (subexpression) struct representing the concatenation node being processed
- : Pointer to the beginning character of the substring to match
- : Pointer to the end character of the substring to match

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the DFA for subexpressions
  - : Finds the shortest possible match for a DFA
  - : Finds the longest possible match for a DFA
  - : Recursively dissects child subexpressions
  - : Resets subexpression matches
  - : Macro for error checking
  - : Macro for debug output
  - : Macro for converting pointers to offsets
- Called from:
  - : Main concatenation dissection dispatcher function

## Notes and Other Information
- This function is specifically designed for concatenation nodes where the left operand has the SHORTER flag set
- The algorithm ensures all possible combinations are tried by advancing the midpoint when a match fails
- Error handling includes checking for REG_NOMATCH, REG_OKAY, and other regex execution return codes
- The function includes extensive debug output to trace the matching process
- Performance optimization through early termination when no valid midpoint can be found