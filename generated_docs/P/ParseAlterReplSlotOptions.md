# ParseAlterReplSlotOptions

## Location
src/backend/replication/walsender.c: 1418 - 1442

## Overview
Parses and validates the options provided to ALTER_REPLICATION_SLOT commands, specifically handling the failover option.

## Definition
```c
static void ParseAlterReplSlotOptions(AlterReplicationSlotCmd *cmd, bool *failover)
```

## Detailed Description
ParseAlterReplSlotOptions is a specialized parser function that processes the extra options provided with ALTER_REPLICATION_SLOT commands. Currently, it handles only the "failover" option, which controls whether a replication slot should be available for failover scenarios.

The function iterates through the options list in the command structure using the foreach_ptr macro, checking each option name and validating that no duplicate options are specified. For the "failover" option, it extracts the boolean value using defGetBoolean and stores it in the provided output parameter.

The function includes error handling for both unrecognized options (which generate an ERROR) and conflicting/redundant options (which also generate syntax errors). This ensures that only valid, non-duplicate options are processed.

## Parameters / Member Variables
- `cmd`: AlterReplicationSlotCmd structure containing the ALTER_REPLICATION_SLOT command with its options list
- `failover`: Output parameter (bool pointer) that receives the value of the failover option

## Dependencies
- Functions called/Symbols referenced:
  - foreach_ptr
  - [defGetBoolean](../d/defGetBoolean.md)
  - ereport (for error handling)
  - elog (for error handling)
- Called from (representative examples):
  - [AlterReplicationSlot](../A/AlterReplicationSlot.md)

## Notes and Other Information
- Currently only supports the "failover" option, but designed to be extensible for future options
- Uses comprehensive error checking to prevent option conflicts and invalid option names
- The failover option controls slot availability during PostgreSQL cluster failover scenarios
- Part of PostgreSQL's logical replication failover feature introduced for high availability setups
- Error messages follow PostgreSQL's standard error reporting conventions with appropriate error codes