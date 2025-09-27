# AlterReplicationSlot

## Location
[src/backend/replication/walsender.c:1443-1455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L1443-L1455)

## Overview
Modifies the configuration of an existing replication slot, specifically handling changes to the failover property.

## Definition
```c
static void AlterReplicationSlot(AlterReplicationSlotCmd *cmd)
```

## Detailed Description
AlterReplicationSlot is a command-level function that handles modifications to existing replication slots in PostgreSQL's streaming replication system. It serves as an orchestrator that coordinates option parsing and the actual slot alteration process.

The function first initializes a failover flag to false, then delegates option parsing to ParseAlterReplSlotOptions to extract and validate the requested changes from the command structure. After successful parsing, it calls the lower-level ReplicationSlotAlter function to apply the changes to the specified slot.

Currently, the primary use case for slot alteration is modifying the failover property, which determines whether a logical replication slot should be available for use during PostgreSQL cluster failover scenarios. This is an important feature for maintaining replication continuity in high availability setups.

## Parameters / Member Variables
- `cmd`: AlterReplicationSlotCmd structure containing the slot name to alter and the options specifying the desired changes

## Dependencies
- Functions called/Symbols referenced:
  - [ParseAlterReplSlotOptions](../P/ParseAlterReplSlotOptions.md)
  - [ReplicationSlotAlter](../R/ReplicationSlotAlter.md)
- Called from (representative examples):
  - [exec_replication_command](../e/exec_replication_command.md)

## Notes and Other Information
- This function acts as a bridge between replication command processing and slot management
- Currently focused on failover property modification but designed to be extensible for future slot properties
- The failover option is particularly important for logical replication in multi-master or failover cluster configurations
- Error handling and validation are primarily delegated to the called functions
- Part of PostgreSQL's enhanced replication features for high availability environments

## Simplified Source

```c
// Simplified version of AlterReplicationSlot
static void AlterReplicationSlot(AlterReplicationSlotCmd *cmd) {
    // Initialize failover flag (default is false)
    bool failover = false;

    // Step 1: Parse command options to extract failover setting
    ParseAlterReplSlotOptions(cmd, &failover);

    // Step 2: Apply the changes to the specified replication slot
    ReplicationSlotAlter(cmd->slotname, failover);
}
```

Key simplifications made:
- Added descriptive comments explaining each step
- Clarified the purpose of the failover variable initialization
- Highlighted the two-step process: parse options, then apply changes
- Maintained the essential logic flow and function calls