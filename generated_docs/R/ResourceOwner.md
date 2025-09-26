# ResourceOwner

## Location
src/include/utils/resowner.h: 27 - 53

## Overview
ResourceOwner is an opaque handle type that represents a resource management object used to track and automatically clean up query-lifespan resources in PostgreSQL.

## Definition


## Detailed Description
ResourceOwner provides PostgreSQL's primary mechanism for tracking and managing resources that need to be cleaned up at specific points during query execution or transaction processing. The actual ResourceOwnerData structure is opaque and only accessible within resowner.c, ensuring encapsulation of the resource management implementation.

The system maintains several globally known ResourceOwners for different contexts:
- CurrentResourceOwner: The currently active resource owner
- CurTransactionResourceOwner: Owner for current transaction resources  
- TopTransactionResourceOwner: Owner for top-level transaction resources
- AuxProcessResourceOwner: Owner for auxiliary process resources

Resources are tracked by associating them with ResourceOwner objects, and cleanup occurs automatically during resource owner release in three phases (pre-locks, locks, post-locks) with configurable priorities.

## Parameters / Member Variables
Since ResourceOwner is an opaque pointer type, the internal structure is not directly accessible. The actual ResourceOwnerData struct contains:
- Parent-child relationships for hierarchical resource management
- Hash tables for tracking different resource types
- Resource arrays and capacity management
- Name for debugging purposes

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerData (internal structure)
  - Various global ResourceOwner instances
- Called from (representative examples):
  - ResourceOwnerCreate
  - ResourceOwnerRelease  
  - ResourceOwnerDelete
  - ResourceOwnerRemember
  - ResourceOwnerForget
  - Portal management functions
  - Transaction management code
  - Memory context management
  - Lock management systems
  - SPI execution functions

## Notes and Other Information
- ResourceOwner objects form a hierarchical tree structure where child owners are released before their parents
- The system automatically handles cleanup during transaction abort, commit, or subtransaction end
- Extensions can register custom resource types by providing ResourceOwnerDesc callbacks
- Resource cleanup occurs in deterministic order based on release phase and priority
- The design ensures that resources visible to other backends are released before locks, preventing deadlocks
- Critical for memory management, file handle cleanup, lock release, and other resource lifecycle management