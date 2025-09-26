# ProcessInvalidationMessagesMulti

## Location
[src/backend/utils/cache/inval.c:527-544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L527-L544)

## Overview
Executes a given function for all invalidation messages in a message group, processing them as arrays rather than individually, with catalog cache messages processed before relation cache messages.

## Definition
```c
static void ProcessInvalidationMessagesMulti(InvalidationMsgsGroup *group, void (*func) (const SharedInvalidationMessage *msgs, int n))
```

## Detailed Description
This function is similar to ProcessInvalidationMessages but provides a more efficient processing method by passing entire arrays of messages to the processing function rather than individual messages. It processes invalidation messages in the same order as ProcessInvalidationMessages: catalog cache messages first, followed by relation cache messages.

The function uses ProcessMessageSubGroupMulti macro to pass arrays of messages from each subgroup to the provided function. This approach is more efficient when the processing function can handle multiple messages at once, such as when sending messages to shared memory or performing batch operations.

## Parameters / Member Variables
- `group`: Pointer to the InvalidationMsgsGroup containing the messages to be processed
- `func`: Function pointer that accepts an array of SharedInvalidationMessage pointers and a count, to be called for each message subgroup

## Dependencies
- Functions called/Symbols referenced:
  - ProcessMessageSubGroupMulti (macro that processes message subgroups as arrays)
  - CatCacheMsgs (catalog cache message subgroup identifier)
  - RelCacheMsgs (relation cache message subgroup identifier)
- Types referenced:
  - [InvalidationMsgsGroup](../I/InvalidationMsgsGroup.md)
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md)
- Called from:
  - [AtEOXact_Inval](../A/AtEOXact_Inval.md) (during transaction end processing for efficient message transmission)

## Notes and Other Information
- This is a static function, only accessible within the inval.c module
- More efficient than ProcessInvalidationMessages when the processing function can handle multiple messages at once
- Maintains the same processing order as ProcessInvalidationMessages (catalog cache first, then relation cache)
- The provided function is typically SendSharedInvalidMessages for efficient transmission to shared memory
- Part of PostgreSQL's invalidation message processing system for maintaining cache coherency
- Used primarily when sending accumulated invalidation messages to other backends via shared memory