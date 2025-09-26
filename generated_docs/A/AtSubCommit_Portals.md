# AtSubCommit_Portals

## Location
[src/backend/utils/mmgr/portalmem.c:943-978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L943-L978)

## Overview
Pre-subcommit processing function that reassigns portals created or used in the current subtransaction to the parent subtransaction during subtransaction commit.

## Definition

```c
void
AtSubCommit_Portals(SubTransactionId mySubid,
					SubTransactionId parentSubid,
					int parentLevel,
					ResourceOwner parentXactOwner)
```
## Detailed Description
AtSubCommit_Portals is called during subtransaction commit to properly transfer portal ownership from the committing subtransaction to its parent. This function iterates through all portals in the portal hash table and updates their subtransaction identifiers and resource ownership. The function ensures that portals created in a subtransaction remain accessible after the subtransaction commits by reassigning them to the parent transaction context.

The function performs two main operations:
1. For portals created in the current subtransaction (createSubid == mySubid), it updates the createSubid to parentSubid, sets the createLevel to parentLevel, and transfers resource ownership to the parent transaction's resource owner.
2. For portals that were active in the current subtransaction (activeSubid == mySubid), it updates the activeSubid to parentSubid.

## Parameters
- : The subtransaction ID of the subtransaction being committed
- : The subtransaction ID of the parent subtransaction
- : The nesting level of the parent subtransaction
- : The resource owner of the parent transaction context

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [ResourceOwnerNewParent](../R/ResourceOwnerNewParent.md)
- Data types used:
  - SubTransactionId
  - [ResourceOwner](../R/ResourceOwner.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [PortalHashEnt](../P/PortalHashEnt.md)
  - [Portal](../P/Portal.md)
- Called from:
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (src/backend/access/transam/xact.c:5091)

## Notes and Other Information
- This function operates on the global PortalHashTable which contains all active portals
- The function is part of the subtransaction commit protocol and ensures portal consistency across transaction boundaries
- Resource ownership transfer is critical for proper cleanup when the parent transaction eventually commits or aborts
- The function is defined in src/backend/utils/mmgr/portalmem.c:943-978