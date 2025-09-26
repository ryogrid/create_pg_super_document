# CreateEmptyBlockRefTable

## Location
src/common/blkreftable.c: 235 - 261

## Overview
Creates an empty block reference table with an initial hash table capacity optimized for typical database usage patterns.

## Definition


## Detailed Description
This function initializes a new BlockRefTable structure with an empty hash table. The function allocates memory for the BlockRefTable structure and creates an underlying hash table with an initial capacity of 4096 entries. This initial sizing is based on the assumption that even a completely empty database will have several hundred relation forks, and the table will likely grow to contain at least a few thousand entries during normal operation.

The function handles both frontend and backend contexts differently:
- In frontend contexts: Creates a simple hash table without memory context tracking
- In backend contexts: Associates the hash table with the current memory context for proper memory management

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - : Memory allocation function
  - : Creates the underlying hash table
  - : Current memory context (backend only)
- Called from (representative examples):
  - : For incremental backup preparation
  - : For WAL summarization operations

## Notes and Other Information
- The initial hash table size of 4096 is a performance optimization based on expected usage patterns
- The function uses conditional compilation (#ifdef FRONTEND) to handle different execution environments
- Memory management differs between frontend and backend: backend version tracks the memory context explicitly
- The BlockRefTable structure encapsulates a hash table while hiding implementation details from callers
- This is typically the first function called when working with block reference tables