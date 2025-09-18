# newstate

## Location
[src/backend/regex/regc_nfa.c:137-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L137-L211)

## Overview
Allocates a new state in an NFA structure with efficient memory management using state batches and free lists.

## Definition


## Detailed Description
The  function creates a new state for an NFA with sophisticated memory management. It first checks for interrupt signals to allow cancellation during compilation. The function uses a three-tier allocation strategy: first attempting to reuse states from a freelist, then using available space in the current state batch, and finally allocating a new batch when needed. State batches grow exponentially (doubling in size) up to a maximum limit to balance memory efficiency with allocation overhead. Each new state is initialized with a unique number, linked into the NFA's state list, and has all fields properly initialized.

## Parameters / Member Variables
- : Pointer to the NFA structure that will contain the new state

## Dependencies
- Functions called/Symbols referenced:
  - INTERRUPT
  - NERR
  - MALLOC
  - STATEBATCHSIZE
  - REG_MAX_COMPILE_SPACE
  - REG_ETOOBIG
  - REG_ESPACE
  - FIRSTSBSIZE
  - MAXSBSIZE
- Called from (representative examples):
  - [newnfa](newnfa.md) (in regc_nfa.c)
  - newfstate (in regc_nfa.c)
  - [duptraverse](../d/duptraverse.md) (in regc_nfa.c)
  - [pull](../p/pull.md) (in regc_nfa.c)
  - push (in regc_nfa.c)
  - [makesearch](../m/makesearch.md) (in regcomp.c)
  - [parse](../p/parse.md) (in regcomp.c)
  - [parsebranch](../p/parsebranch.md) (in regcomp.c)

## Notes and Other Information
- Returns NULL on memory allocation failure or space limit exceeded
- Implements exponential growth strategy for state batches to optimize allocation
- Maintains both forward and backward linked lists for efficient state traversal
- Includes interrupt checking to support query cancellation during regex compilation
- Each state gets a unique sequential number for identification
- Properly initializes all state fields including ins/outs arrays and temporary pointers
- Critical function called frequently during NFA construction for complex regex patterns