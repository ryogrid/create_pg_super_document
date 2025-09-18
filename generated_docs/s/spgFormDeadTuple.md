# spgFormDeadTuple

## Location
src/backend/access/spgist/spgutils.c: 1077 - 1106

## Overview
Constructs a "dead" tuple to replace a tuple being deleted in SP-GiST indexes, supporting different states including redirect, dead, and placeholder tuples.

## Definition


## Detailed Description
The  function creates a special dead tuple that replaces existing tuples during deletion operations in SP-GiST indexes. This function supports three different tuple states:  (points to a new location),  (marks tuple as deleted), and  (temporary marker). For redirect tuples, it stores the target location and transaction ID, while other states use invalid pointers.

The function is designed to be called within critical sections, so it uses preallocated storage () rather than dynamic allocation via . This ensures atomic operations and prevents memory allocation failures during critical index operations.

## Parameters / Member Variables
- : Pointer to SpGistState containing index configuration and preallocated dead tuple storage
- : The state of the dead tuple (SPGIST_REDIRECT, SPGIST_DEAD, or SPGIST_PLACEHOLDER)
- : Block number for redirect target (only used for SPGIST_REDIRECT state)
- : Offset number for redirect target (only used for SPGIST_REDIRECT state)

## Dependencies
- Functions called/Symbols referenced:
  -  - macro to set the next offset field
  -  - sets item pointer to specific block and offset
  -  - marks item pointer as invalid
  -  - constant defining dead tuple size
  -  - constant for invalid offset
  -  - constant for invalid transaction ID
- Called from (representative examples):
  -  - during multi-tuple deletion operations
  -  - when adding nodes requires replacing existing tuples
  -  - during WAL replay of add node operations

## Notes and Other Information
- Uses preallocated storage to avoid memory allocation in critical sections
- The resulting tuple must be copied before making another call with different parameters
- Redirect tuples store both target location and the transaction ID that created the redirect
- Non-redirect tuples have invalid pointers and transaction IDs to indicate they don't point anywhere
- The fixed size (SGDTSIZE) ensures compatibility with tuple replacement operations
- Critical for maintaining index consistency during concurrent operations and crash recovery