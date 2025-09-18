# MemoryContextCreate

## Location
src/backend/utils/mmgr/mcxt.c: 1100 - 1146

## Overview
MemoryContextCreate is the context-type-independent core function for creating memory contexts in PostgreSQL, responsible for initializing the generic header fields and linking new contexts into the context tree.

## Definition


## Detailed Description
This function serves as the foundation for all memory context creation in PostgreSQL. It's designed to be called only by context-type-specific creation routines (like AllocSetContextCreate, BumpContextCreate, etc.), not directly by general application code.

The function follows a carefully orchestrated creation procedure:
1. Context-specific routines first allocate initial space including the context header
2. They set up type-specific fields and management structures
3. MemoryContextCreate is called to initialize generic header fields and link the context
4. Context-specific routines complete any remaining initialization

The function initializes all standard fields of the memory context header, establishes parent-child relationships in the context tree, and ensures proper inheritance of critical section permissions. It also integrates with Valgrind for memory debugging support.

## Parameters / Member Variables
- : The uninitialized common part of the context header node to be initialized
- : NodeTag code that identifies the specific memory context type being created
- : MemoryContextMethodID specifying which context implementation methods to use
- : Parent memory context, or NULL if creating a top-level context
- : Human-readable name for the context (must be statically allocated string)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextMethodID (for method dispatch table access)
  - VALGRIND_CREATE_MEMPOOL (for memory debugging integration)
- Called from (representative examples):
  - AllocSetContextCreateInternal
  - BumpContextCreate
  - GenerationContextCreate
  - SlabContextCreate

## Notes and Other Information
- This function assumes it cannot fail and uses Assert rather than elog/ereport for error conditions
- Creating new memory contexts is prohibited in critical sections (Assert(CritSectionCount == 0))
- The allowInCritSection flag is inherited from the parent context, defaulting to false for top-level contexts
- The function establishes bidirectional parent-child links for efficient context tree traversal
- Context names must be statically allocated strings to avoid memory management complications
- Integration with Valgrind mempool tracking aids in debugging memory-related issues