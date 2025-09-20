# StackElem

## Location
[src/include/fe_utils/psqlscan_int.h:69-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/psqlscan_int.h#L69-L76)

## Overview
StackElem is a struct that represents a single element in the variable expansion buffer stack used by PostgreSQL's lexical scanner to handle psql variable substitution.

## Definition

```c
typedef struct StackElem
{
	YY_BUFFER_STATE buf;		/* flex input control structure */
	char	   *bufstring;		/* data actually being scanned by flex */
	char	   *origstring;		/* copy of original data, if needed */
	char	   *varname;		/* name of variable providing data, or NULL */
	struct StackElem *next;
} StackElem;
```
## Detailed Description
StackElem is a fundamental component of PostgreSQL's variable substitution mechanism in the psql command-line interface. It implements a stack-based approach to handle nested variable expansions, where each stack element contains the necessary information to manage a single variable's content during lexical scanning.

The stack design allows psql to handle complex scenarios where variables contain references to other variables, enabling proper expansion and restoration of the scanning context. When a variable is encountered during scanning, a new StackElem is pushed onto the stack with the variable's content. When the variable content is fully processed, the stack element is popped, and scanning resumes from the previous context.

This mechanism is essential for PostgreSQL's flexible variable system, allowing users to define and nest variables in psql scripts and interactive sessions.

## Parameters / Member Variables
- `buf`: YY_BUFFER_STATE that holds the flex input control structure for this buffer, managing the actual scanning state
- `*bufstring`: Pointer to the string data that flex is currently scanning from this stack element
- `*origstring`: Copy of the original data before any encoding transformations, used when dealing with non-ASCII-safe multibyte encodings
- `*varname`: Name of the psql variable that provided this data, or NULL if this buffer represents non-variable content
- `*next`: Pointer to the next element in the stack, forming a linked list structure for the buffer stack
## Dependencies
- Functions called/Symbols referenced:
  - [YY_BUFFER_STATE](../Y/YY_BUFFER_STATE.md) (used for buf member)
  - [StackElem](StackElem.md) (self-reference for next pointer)
- Called from (representative examples):
  - [PsqlScanStateData](../P/PsqlScanStateData.md) (used as buffer_stack member)
  - psqlscan_push_new_buffer (manipulates stack elements)
  - psqlscan_pop_buffer_stack (manipulates stack elements)

## Notes and Other Information
- Forms a linked list structure to implement a stack for variable expansion contexts
- Essential for handling nested variable references in psql
- The stack design ensures proper restoration of scanning context after variable expansion
- Works in conjunction with PostgreSQL's multibyte encoding handling system
- Part of the re-entrant lexer architecture that supports multiple simultaneous scanning operations
- Used exclusively in the frontend utilities, not in the backend server code