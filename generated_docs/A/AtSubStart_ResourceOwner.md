# AtSubStart_ResourceOwner

## Location
[src/backend/access/transam/xact.c:1272-1303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1272-L1303)

## Overview
AtSubStart_ResourceOwner initializes the resource owner for a new subtransaction, creating a hierarchical structure where the subtransaction's resource owner is a child of its parent transaction's resource owner.

## Definition
```c
static void AtSubStart_ResourceOwner(void)
```

## Detailed Description
This static function is called during subtransaction startup to establish proper resource management for the new subtransaction. It creates a new ResourceOwner object that serves as a child of the parent transaction's resource owner, ensuring that resources allocated by the subtransaction are properly tracked and can be cleaned up if the subtransaction aborts. The function updates both the transaction state's curTransactionOwner and the global resource owner variables (CurTransactionResourceOwner and CurrentResourceOwner) to point to the newly created resource owner.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerCreate](../R/ResourceOwnerCreate.md)
  - TransactionState (type)
- Called from (representative examples):
  - [StartSubTransaction](../S/StartSubTransaction.md)

## Notes and Other Information
- This is a static function within xact.c, part of the subtransaction initialization sequence
- The resource owner hierarchy ensures that subtransaction resources are properly nested under their parent transaction
- The "SubTransaction" label is used for debugging and identification purposes
- Critical for maintaining PostgreSQL's nested transaction model and resource cleanup guarantees