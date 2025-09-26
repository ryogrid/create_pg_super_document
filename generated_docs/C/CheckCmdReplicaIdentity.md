# CheckCmdReplicaIdentity

## Location
[src/backend/executor/execReplication.c:656-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L656-L742)

## Overview
CheckCmdReplicaIdentity validates whether UPDATE or DELETE commands can be safely executed based on the relations replica identity configuration and publication settings.

## Definition
```c
void CheckCmdReplicaIdentity(Relation rel, CmdType cmd)
```

## Detailed Description
This function performs comprehensive validation to ensure that UPDATE and DELETE operations are safe to execute in a logical replication environment. It validates the relationship between a tables replica identity, its publication configuration, and the specific command being executed.

The function implements several layers of validation: first checking if the relation is a partitioned table (which is skipped since operations occur on leaf partitions), then validating that row filters and column lists in publications are compatible with the replica identity. It ensures that all columns referenced in publication WHERE expressions and column lists are covered by the replica identity.

If a table lacks a proper replica identity (neither a replica identity index nor REPLICA IDENTITY FULL), the function checks whether the table publishes the requested operation type. If it does, the function raises an error with helpful hints about configuring replica identity using ALTER TABLE.

The validation is essential for maintaining data consistency in logical replication scenarios where the subscriber needs to reliably identify and modify the correct tuples.

## Parameters / Member Variables
- `rel`: The relation being checked for replica identity compatibility
- `cmd`: The command type being validated (CMD_INSERT, CMD_UPDATE, CMD_DELETE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md): Builds publication descriptor containing validity flags and publication actions
  - [RelationGetReplicaIndex](../R/RelationGetReplicaIndex.md): Gets the replica identity index OID for the relation
  - RelationGetRelationName: Gets the relation name for error messages
  - OidIsValid: Checks if the replica identity index OID is valid
- Called from (representative examples):
  - [CheckValidResultRel](CheckValidResultRel.md): General result relation validation in executor
  - [ExecSimpleRelationInsert](../E/ExecSimpleRelationInsert.md): Replica identity validation for INSERT operations
  - [ExecSimpleRelationUpdate](../E/ExecSimpleRelationUpdate.md): Replica identity validation for UPDATE operations  
  - [ExecSimpleRelationDelete](../E/ExecSimpleRelationDelete.md): Replica identity validation for DELETE operations
  - [exec_rt_fetch](../e/exec_rt_fetch.md): Through executor header inclusion

## Notes and Other Information
- Only validates UPDATE and DELETE commands - INSERT operations are always allowed
- Skips validation for partitioned tables since operations occur on leaf partitions
- Provides comprehensive error messages with specific details about column coverage issues
- Includes helpful hints about using ALTER TABLE to set REPLICA IDENTITY when needed
- Supports both replica identity indexes and REPLICA IDENTITY FULL configurations
- Critical for logical replication consistency and data integrity
- Validates both row filter expressions and publication column lists against replica identity
- Uses PublicationDesc structure to encapsulate publication validation state