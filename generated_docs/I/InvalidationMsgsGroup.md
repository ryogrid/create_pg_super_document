# InvalidationMsgsGroup

## Location
[src/backend/utils/cache/inval.c:175-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L175-L179)

## Overview
InvalidationMsgsGroup is a control structure that manages logical groups of cache invalidation messages, providing index boundaries for both catalog cache and relation cache message subgroups.

## Definition


## Detailed Description
InvalidationMsgsGroup serves as a control structure to organize and track boundaries of invalidation message groups within PostgreSQL's cache invalidation system. It maintains index ranges for two types of invalidation messages: catalog cache messages (CatCacheMsgs, index 0) and relation cache messages (RelCacheMsgs, index 1).

The structure defines logical boundaries within the InvalMessageArrays, allowing the system to group related invalidation messages together. This is particularly important for transaction processing where different commands and subtransactions need to track their invalidation messages separately.

Each group represents a range of messages in the corresponding InvalMessageArray, where firstmsg[i] points to the first message in subgroup i, and nextmsg[i] points to one position past the last message (following the standard C convention for ranges).

## Parameters / Member Variables
- : Array containing the first index in the relevant message array for each subgroup (0=CatCacheMsgs, 1=RelCacheMsgs)
- : Array containing the last+1 index (exclusive end boundary) for each subgroup, following C convention where the range is [firstmsg, nextmsg)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - used as a data container)
- Called from (representative examples):
  - [TransInvalidationInfo](../T/TransInvalidationInfo.md) (contains two instances: CurrentCmdInvalidMsgs and PriorCmdInvalidMsgs)
  - [AddInvalidationMessage](../A/AddInvalidationMessage.md)
  - [AppendInvalidationMessageSubGroup](../A/AppendInvalidationMessageSubGroup.md)
  - [AddCatcacheInvalidationMessage](../A/AddCatcacheInvalidationMessage.md)
  - [AddCatalogInvalidationMessage](../A/AddCatalogInvalidationMessage.md)
  - [AddRelcacheInvalidationMessage](../A/AddRelcacheInvalidationMessage.md)
  - [ProcessInvalidationMessages](../P/ProcessInvalidationMessages.md)

## Notes and Other Information
- Used within TransInvalidationInfo to separate messages from current command vs. previous commands
- Supports nested transaction handling by allowing subtransaction message grouping
- Works with helper macros SetSubGroupToFollow, SetGroupToFollow, NumMessagesInSubGroup, and NumMessagesInGroup
- The two-element arrays correspond to CatCacheMsgs (index 0) and RelCacheMsgs (index 1) defined as constants
- Essential for proper transaction rollback and commit processing of cache invalidations
- Enables efficient batch processing of invalidation messages by maintaining clear group boundaries