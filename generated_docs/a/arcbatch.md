# arcbatch

## Location
src/include/regex/regguts.h: 311 - 316

## Overview
The  structure is used for bulk allocation of arc structures in PostgreSQL's regular expression engine, providing efficient memory management for NFA (Non-deterministic Finite Automaton) arcs.

## Definition


## Detailed Description
The  structure implements a memory pool mechanism for efficiently allocating multiple arc structures at once. This approach reduces memory fragmentation and allocation overhead when creating many arcs for regular expression NFAs. The structure uses a linked list design where each batch can hold a variable number of arc structures, with the actual arcs stored in a flexible array member at the end of the structure.

## Parameters / Member Variables
- : Pointer to the next arcbatch in the chain, forming a linked list of batches
- : The number of arc structures allocated in this particular batch
- : Flexible array member containing the actual arc structures

## Dependencies
- Functions called/Symbols referenced:
  -  (struct arc for the flexible array member)
  -  (macro for flexible array declaration)
- Called from (representative examples):
  -  (for cleanup and deallocation)
  -  (for arc allocation from batches)
  -  (macro that calculates batch size)

## Notes and Other Information
- The structure is part of PostgreSQL's regex engine located in src/include/regex/regguts.h
- Uses the FLEXIBLE_ARRAY_MEMBER technique for variable-length allocation
- The ARCBATCHSIZE macro calculates the total size needed for a batch with n arcs
- This bulk allocation strategy improves performance when creating large NFAs with many transitions
- Memory is managed through a chain of batches, allowing for efficient allocation and cleanup