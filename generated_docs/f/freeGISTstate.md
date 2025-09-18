# freeGISTstate

## Location
src/backend/access/gist/gist.c: 1660 - 1670

## Overview
Frees memory allocated for a GISTSTATE structure by deleting its scan-lifespan memory context.

## Definition


## Detailed Description
The  function is responsible for cleaning up memory allocated for a GISTSTATE structure. It works by deleting the  memory context, which contains the GISTSTATE itself and any data that lives for the lifetime of the index operation. Since PostgreSQL's memory context system automatically frees all memory allocated within a context when the context is deleted, this single operation effectively frees all memory associated with the GISTSTATE.

This function is part of PostgreSQL's GiST (Generalized Search Tree) index access method implementation and is typically called when an index operation completes to prevent memory leaks.

## Parameters / Member Variables
- : Pointer to the GISTSTATE structure to be freed. The structure contains scan-lifespan data and function manager information for GiST index operations.

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextDelete
  - GISTSTATE
- Called from (representative examples):
  - gistbuild
  - gistendscan

## Notes and Other Information
- The function relies on PostgreSQL's memory context system for efficient memory management
- Only the scanCxt memory context needs to be deleted as it contains all scan-lifespan data
- This is a cleanup function typically called at the end of GiST index operations
- The GISTSTATE structure contains function manager information for opclass-specific support functions and tuple descriptors for the index