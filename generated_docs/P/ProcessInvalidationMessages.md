# ProcessInvalidationMessages

## Location
src/backend/utils/cache/inval.c: 515 - 526

## Overview
Executes a given function for all invalidation messages in a message group, processing catalog cache messages first followed by relation cache messages.

## Definition
```c
static void ProcessInvalidationMessages(InvalidationMsgsGroup *group, void (*func) (SharedInvalidationMessage *msg))
```

## Detailed Description
This function processes all invalidation messages within an InvalidationMsgsGroup by applying a provided function to each message. The function processes messages in a specific order: catalog cache messages (CatCacheMsgs) are processed first, followed by relation cache messages (RelCacheMsgs). This ordering is important because catalog cache invalidations may affect the interpretation of subsequent relation cache invalidations.

The function uses the ProcessMessageSubGroup macro to iterate through each message subgroup and apply the provided function. The message group itself is not modified during processing - it serves as a read-only source of messages to be processed.

## Parameters / Member Variables
- `group`: Pointer to the InvalidationMsgsGroup containing the messages to be processed
- `func`: Function pointer to be called for each SharedInvalidationMessage in the group

## Dependencies
- Functions called/Symbols referenced:
  - ProcessMessageSubGroup (macro that iterates through message subgroups)
  - CatCacheMsgs (catalog cache message subgroup identifier)
  - RelCacheMsgs (relation cache message subgroup identifier)
- Types referenced:
  - InvalidationMsgsGroup
  - SharedInvalidationMessage
- Called from:
  - AtEOXact_Inval (during transaction end processing)
  - AtEOSubXact_Inval (during subtransaction end processing)
  - CommandEndInvalidationMessages (during command completion)

## Notes and Other Information
- This is a static function, only accessible within the inval.c module
- The processing order (catalog cache first, then relation cache) is deliberately chosen for correctness
- The function does not modify the message group - it's a read-only operation
- The provided function is typically LocalExecuteInvalidationMessage or a similar message handler
- Part of PostgreSQL's invalidation message processing system for maintaining cache coherency
- Used primarily during transaction state transitions to apply accumulated invalidations