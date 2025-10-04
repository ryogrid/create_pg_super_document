# parse

## Location
[src/tools/pg_bsd_indent/parse.c:49-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/parse.c#L49-L259)

## Overview
The parse function is the top-level parser for regular expressions in PostgreSQL's regex engine, handling the parsing of multiple branches connected by the '|' (alternation) operator.

## Definition

```c
void
parse(int tk) /* tk: the code for the construct scanned */
```
## Detailed Description
The parse function serves as the top-level parser in PostgreSQL's regular expression compilation system. It processes a regular expression by parsing multiple branches that are connected with the '|' alternation operator. When multiple branches exist, they are represented in the parse tree as children of a '|' subre (sub-regular expression) node.

The function creates a branching structure in the NFA (Non-deterministic Finite Automaton) by:
1. Creating a top-level '|' subre node to hold all branches
2. For each branch separated by '|', creating new states and calling parsebranch to handle the detailed parsing
3. Linking branches together as siblings in the parse tree
4. Optimizing simple cases where only one branch exists or no complex features are used

## Parameters / Member Variables
- : Pointer to the regex compilation variables and state
- : Character that indicates the end of parsing (either EOS for end-of-string or ')' for end of group)
- : Type of sub-regular expression being parsed (LACON for lookaround assertions or PLAIN for normal expressions)
- : Initial state in the NFA for this parse unit
- : Final state in the NFA for this parse unit

## Dependencies
- Functions called/Symbols referenced:
  - [subre](../s/subre.md): Creates new sub-regular expression nodes
  - [newstate](../n/newstate.md): Creates new NFA states
  - EMPTYARC: Creates empty transitions between states
  - [parsebranch](parsebranch.md): Parses individual branches of the alternation
  - [freesrnode](../f/freesrnode.md): Frees sub-regular expression nodes
  - [freesubreandsiblings](../f/freesubreandsiblings.md): Frees entire chains of sub-expressions
  - Various macro utilities: NOERR, EAT, SEE, UP, MESSY, ERR
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- The function includes optimization logic that simplifies the parse tree when only one branch exists or when branches contain no complex features
- Error handling is integrated throughout with assertions and error reporting for malformed expressions
- The function is part of PostgreSQL's regex engine located in src/backend/regex/regcomp.c
- Uses a bottom-up parsing approach where branches are parsed individually and then combined into alternation structures

## Simplified Source

```c
static struct subre *
parse(struct vars *v, int stopper, int type, struct state *init, struct state *final)
{
    struct subre *branches;     // Top-level alternation node
    struct subre *lastbranch;   // Track last processed branch

    // Create top-level '|' node to hold all branches
    branches = subre(v, '|', LONGER, init, final);
    NOERRN();
    lastbranch = NULL;

    // Parse each branch separated by '|'
    do {
        struct subre *branch;
        struct state *left, *right;

        // Create states for this branch
        left = newstate(v->nfa);
        right = newstate(v->nfa);
        NOERRN();

        // Connect states with empty arcs
        EMPTYARC(init, left);
        EMPTYARC(right, final);
        NOERRN();

        // Parse this branch
        branch = parsebranch(v, stopper, type, left, right, 0);
        NOERRN();

        // Link branch into tree structure
        if (lastbranch)
            lastbranch->sibling = branch;
        else
            branches->child = branch;

        // Update flags from branch
        branches->flags |= UP(branches->flags | branch->flags);
        lastbranch = branch;

    } while (EAT('|'));  // Continue while seeing '|'

    // Validate proper termination
    if (!SEE(stopper)) {
        ERR(REG_EPAREN);
    }

    // Optimize simple cases
    if (lastbranch == branches->child) {
        // Only one branch - eliminate unnecessary alternation
        freesrnode(v, branches);
        branches = lastbranch;
    } else if (!MESSY(branches->flags)) {
        // No complex features - simplify to basic match
        freesubreandsiblings(v, branches->child);
        branches->child = NULL;
        branches->op = '=';
    }

    return branches;
}
```