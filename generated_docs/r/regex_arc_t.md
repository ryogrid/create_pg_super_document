# regex_arc_t

## Location
[src/include/regex/regexport.h:42-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regexport.h#L42-L61)

## Overview
Represents a single arc (transition) in a regular expression's Non-deterministic Finite Automaton (NFA), containing the character-set color label and destination state.

## Definition

```c
typedef struct
{
	int			co;				/* label (character-set color) of arc */
	int			to;				/* next state number */
} regex_arc_t;
```
## Detailed Description
The  structure is a fundamental component of PostgreSQL's regex NFA export functionality. It represents a directed transition between states in the NFA, where each arc is labeled with a "color" that represents one or more concrete character codes. The NFA uses colors to group characters that are treated equivalently by the regex pattern, allowing for efficient representation and processing.

This structure is used to expose the internal NFA structure to external code that needs to analyze or traverse the compiled regular expression's state machine. The exported NFA provides a necessary but not sufficient condition for string matching - strings that match the NFA may not match the full regex due to additional constraints like lookaround assertions, but strings that don't match the NFA definitely won't match the regex.

## Parameters / Member Variables
- `co`: The color (character-set label) of the arc. Color 0 represents "white" (unused characters), while other colors represent specific character sets. Special pseudocolors represent start/end of line and start/end of string conditions.
- `to`: The destination state number that this arc leads to in the NFA. State numbers range from 0 to N-1 where N is the total number of states.
## Dependencies
- Functions called/Symbols referenced:
  - Used within  function calls
  - Referenced in  internal function
- Called from (representative examples):
  -  (returns arrays of regex_arc_t)
  -  (fills regex_arc_t arrays during LACON traversal)

## Notes and Other Information
- The structure is designed for external access to regex internals and is part of the public regex export API
- LACON (Lookaround Constraint) arcs are masked from external callers and automatically traversed internally
- Colors are numbered 0 to C-1, with color 0 being special "white" color for unused characters
- The NFA may contain multiple arcs with the same color from a single state since it's non-deterministic
- This structure is used in conjunction with regex state information functions to provide complete NFA traversal capabilities