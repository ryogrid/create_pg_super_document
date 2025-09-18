# gistInitParentMap

## Location
src/backend/access/gist/gistbuild.c: 1514 - 1527

## Overview
Initializes a hash table that maps block numbers to parent information during GiST index construction.

## Definition


## Detailed Description
This function creates and configures a hash table within the GiST build state that serves as a parent map during index construction. The hash table maps block numbers (BlockNumber) to parent map entries (ParentMapEntry), allowing the build process to efficiently track parent-child relationships between index pages. The hash table is configured with specific parameters optimized for the GiST build process, including the use of blob-based hashing for block numbers and a custom memory context.

## Parameters / Member Variables
- : Pointer to the GISTBuildState structure that maintains the overall state during GiST index construction. The function initializes the parentMap field within this structure.

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - HASHCTL
  - ParentMapEntry
  - HASH_ELEM
  - HASH_BLOBS
  - HASH_CONTEXT
- Called from (representative examples):
  - [gistInitBuffering](gistInitBuffering.md)

## Notes and Other Information
- The hash table is created with an initial size of 1024 entries
- Uses blob-based hashing (HASH_BLOBS) since BlockNumber is a simple numeric type
- The hash table uses the current memory context for allocations
- This is a static function, only accessible within the gistbuild.c file
- The parent map is essential for maintaining index structure integrity during the buffering-based build process