# parsebranch

## Location
[src/backend/regex/regcomp.c:785-837](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L785-L837)

## Overview
Parses a single branch of a regular expression by managing concatenation of atoms and handling the structural organization of sequential regex components.

## Definition

```c
struct vars *v,
			int stopper,		/* EOS or ')' */
			int type,			/* LACON (lookaround subRE) or PLAIN */
			struct state *left, /* leftmost state */
			struct state *right,	/* rightmost state */
			int partial)		/* is this only part of a branch? */
{
	struct state *lp;			/* left end of current construct */
	int			seencontent;	/* is there anything in this branch yet? */
	struct subre *t;

	lp = left;
	seencontent = 0;
	t = subre(v, '=', 0, left, right);	/* op '=' is tentative */
	NOERRN();
	while (!SEE('|') && !SEE(stopper) && !SEE(EOS))
	{
		if (seencontent)
		{						/* implicit concat operator */
			lp = newstate(v->nfa);
			NOERRN();
			moveins(v->nfa, right, lp);
		}
		seencontent = 1;

		/* NB, recursion in parseqatom() may swallow rest of branch */
		t = parseqatom(v, stopper, type, lp, right, t);
		NOERRN();
	}

	if (!seencontent)
	{							/* empty branch */
		if (!partial)
			NOTE(REG_UUNSPEC);
		assert(lp == left);
		EMPTYARC(left, right);
	}

	return t;
}

/*
 * parseqatom - parse one quantified atom or constraint of an RE
 *
 * The bookkeeping near the end cooperates very closely with parsebranch();
```
## Detailed Description
The  function is responsible for parsing individual branches within regular expressions, primarily focusing on concatenation management. It works closely with  to process sequences of regex atoms (characters, groups, quantifiers, etc.) and bundles them together as efficiently as possible.

The function operates by:
1. Creating a tentative subre node with '=' operation to represent the branch
2. Iteratively parsing individual atoms using  until encountering a branch terminator ('|', stopper, or EOS)
3. For each atom after the first, creating intermediate states to handle concatenation by moving transitions from the right state to a new intermediate state
4. Handling special cases like empty branches with appropriate warnings and empty arc creation

The parser implements intelligent state management for concatenation by using intermediate states () that evolve as atoms are added, ensuring proper NFA connectivity while minimizing unnecessary structure.

## Parameters / Member Variables
- : Pointer to vars structure containing regex compilation context and NFA
- : Character that terminates parsing - either ')' for subexpressions or EOS for end-of-string
- : Type of subexpression being parsed - LACON for lookaround expressions or PLAIN for normal expressions
- : Leftmost state in the NFA for this branch
- : Rightmost state in the NFA for this branch
- : Boolean flag indicating if this is only part of a larger branch (affects empty branch handling)

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates sub-regular expression nodes
  -  - Creates new NFA states for concatenation points
  -  - Moves incoming arcs from one state to another
  -  - Parses individual quantified atoms within the branch
  - / - Error checking macros
  -  - Checks for specific characters without consuming them
  -  - Creates empty transitions between states
  -  - Issues warnings for regex patterns
  - Constants: , 
- Called from (representative examples):
  -  (src/backend/regex/regcomp.c:743)
  -  (src/backend/regex/regcomp.c:1375)

## Notes and Other Information
- Manages concatenation by creating intermediate states only when necessary for proper NFA structure
- Uses  flag to track whether any atoms have been processed in the branch
- The  parameter affects empty branch handling - full branches get warnings, partial branches don't
- Recursion occurs through  which may consume the remainder of the branch in complex cases
- Empty branches result in direct empty arcs between left and right states
- The '=' operation is initially tentative and may be modified by  based on branch complexity
- State management ensures proper concatenation semantics while maintaining efficient NFA structure