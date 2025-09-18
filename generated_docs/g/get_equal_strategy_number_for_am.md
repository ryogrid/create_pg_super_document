# get_equal_strategy_number_for_am

## Location
src/backend/executor/execReplication.c: 49 - 74

## Overview
Returns the fixed strategy number for the equality operator of a given index access method, supporting only B-tree and Hash indexes.

## Definition
```c
StrategyNumber get_equal_strategy_number_for_am(Oid am)
```

## Detailed Description
This function provides the strategy number that represents the equality operation for specific index access methods. It currently supports only B-tree and Hash indexes, which have fixed strategy numbers for equality operations. For other index access methods, the function returns InvalidStrategy because these methods don't have standardized equality strategy numbers - their operator classes define strategy numbers according to their own specifications.

The function uses a simple switch statement to map access method OIDs to their corresponding equality strategy numbers:
- B-tree indexes use BTEqualStrategyNumber
- Hash indexes use HTEqualStrategyNumber
- All other access methods return InvalidStrategy

## Parameters / Member Variables
- `am`: OID of the index access method for which to retrieve the equality strategy number

## Dependencies
- Functions called/Symbols referenced:
  - BTEqualStrategyNumber (constant)
  - HTEqualStrategyNumber (constant) 
  - InvalidStrategy (constant)
  - StrategyNumber (type)

- Called from (representative examples):
  - [get_equal_strategy_number](get_equal_strategy_number.md)
  - [IsIndexUsableForReplicaIdentityFull](../I/IsIndexUsableForReplicaIdentityFull.md)
  - exec_rt_fetch

## Notes and Other Information
- Only B-tree (BTREE_AM_OID) and Hash (HASH_AM_OID) access methods are currently supported
- This function is part of the replication infrastructure in PostgreSQL
- The limitation to B-tree and Hash indexes is explicitly documented in the code comments
- Other index types like GIN, GiST, SP-GiST, and BRIN are not supported because they don't have fixed equality strategy numbers