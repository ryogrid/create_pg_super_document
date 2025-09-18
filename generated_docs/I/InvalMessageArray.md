# InvalMessageArray

## Location
[src/backend/utils/cache/inval.c:166-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L166-L170)

## Overview
InvalMessageArray is a data structure that manages a dynamically expandable array of shared invalidation messages used by PostgreSQL's cache invalidation system.

## Definition


## Detailed Description
InvalMessageArray provides a container for storing SharedInvalidationMessage objects in PostgreSQL's cache invalidation system. It maintains a palloc'd array that can be dynamically expanded as needed. The structure is used within the TopTransactionContext to store invalidation messages that need to be processed for maintaining cache consistency.

The array supports dynamic growth when additional capacity is needed. When the array reaches its maximum capacity, it is reallocated with double the current size to accommodate more messages. Initial allocation starts with 32 elements.

## Parameters / Member Variables
- : Pointer to a palloc'd array of SharedInvalidationMessage structures that can be expanded when needed
- : Integer indicating the current allocated size of the msgs array

## Dependencies
- Functions called/Symbols referenced:
  - SharedInvalidationMessage
- Called from (representative examples):
  - [AddInvalidationMessage](../A/AddInvalidationMessage.md)

## Notes and Other Information
- The structure is used as part of a static array InvalMessageArrays[2] where index 0 represents CatCacheMsgs and index 1 represents RelCacheMsgs
- Memory allocation occurs in TopTransactionContext to ensure proper lifetime management
- Dynamic expansion follows a doubling strategy to minimize reallocation overhead
- Initial allocation size is set to 32 messages (arbitrary choice)
- Used in PostgreSQL's cache invalidation mechanism to batch invalidation messages efficiently