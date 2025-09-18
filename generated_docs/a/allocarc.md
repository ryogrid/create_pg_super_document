# allocarc

## Location
src/backend/regex/regc_nfa.c: 368 - 417

## Overview
Allocates memory for a new arc within an NFA using a memory management strategy that includes recycling freed arcs and batch allocation for efficiency.

## Definition
```c
static struct arc *allocarc(struct nfa *nfa)
```

## Detailed Description
The allocarc function implements a sophisticated memory allocation strategy for arc structures in the regex NFA. It follows a three-tier allocation approach: first attempting to reuse freed arcs from a freelist, then using remaining space in the current arc batch, and finally allocating a new arc batch when needed. The function uses exponential growth for batch sizes (doubling each time) up to a maximum limit, which helps balance memory efficiency with allocation overhead. It also enforces memory limits to prevent excessive resource consumption during regex compilation.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure that needs a new arc allocated
- Returns: Pointer to the allocated arc structure, or NULL on failure

## Dependencies
- Functions called/Symbols referenced:
  - NERR (for error reporting)
  - MALLOC (for memory allocation)
  - REG_MAX_COMPILE_SPACE (compilation space limit constant)
  - REG_ETOOBIG (error code for exceeding space limits)
  - REG_ESPACE (error code for out of space)
  - FIRSTABSIZE (initial arc batch size constant)
  - MAXABSIZE (maximum arc batch size constant)
  - ARCBATCHSIZE (macro for calculating batch size)
- Called from (representative examples):
  - createarc (the primary caller for arc creation)

## Notes and Other Information
- Uses a three-tier memory allocation strategy for optimal performance
- Implements freelist recycling to reuse previously freed arc memory
- Uses batch allocation with exponential growth (doubling) up to MAXABSIZE
- Enforces compilation space limits to prevent runaway memory usage
- Maintains a linked list of arc batches for memory management
- The freelist mechanism helps reduce memory fragmentation
- Arc batches grow exponentially to balance allocation overhead with memory efficiency
- Returns NULL on allocation failure, with appropriate error codes set via NERR