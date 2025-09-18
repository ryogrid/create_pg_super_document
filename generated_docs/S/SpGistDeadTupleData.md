# SpGistDeadTupleData

## Location
src/include/access/spgist_private.h: 427 - 434

## Overview
SpGistDeadTupleData represents the structure for examining non-live tuples in SP-GiST indexes, including dead and redirect tuples that maintain referential integrity during index operations.

## Definition


## Detailed Description
SpGistDeadTupleData defines the structure for non-live tuples in SP-GiST indexes, serving as a specialized format for handling dead and redirect tuples. This structure maintains field compatibility with regular leaf tuples to support safe tuple replacement operations while providing additional metadata for transaction tracking and index redirection. The design ensures that dead tuples can be properly managed during index maintenance operations while preserving the ability to redirect references to new locations when needed.

## Parameters / Member Variables
- : 2-bit field indicating tuple state (LIVE/REDIRECT/DEAD/PLACEHOLDER), must match regular tuple formats
- : 30-bit size field matching leaf tuple format requirements for compatibility
- : 16-bit field not used in dead tuples but required for field alignment with leaf tuples
- : ItemPointerData for redirection within the index, valid only when tupstate = REDIRECT
- : TransactionId of the transaction that inserted this tuple, valid for REDIRECT state and may be InvalidTransactionId in some cases

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerData (implicitly referenced)
  - TransactionId (implicitly referenced)
- Called from (representative examples):
  - SpGistDeadTuple
  - SGDTSIZE

## Notes and Other Information
- Field layout must maintain compatibility with regular inner and leaf tuples for safe replacement operations
- The pointer field position matches leaf tuple heapPtr field to satisfy replacement assertions
- Transaction ID tracking enables proper visibility and cleanup of redirect tuples
- t_info field is unused but necessary for maintaining proper field alignment
- REDIRECT state tuples use pointer and xid fields, while other states may leave them undefined
- Structure supports index reorganization scenarios where tuples need to be marked as dead or redirected