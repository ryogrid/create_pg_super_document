# BaseBackupTargetType

## Location
[src/backend/backup/basebackup_target.c:21-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_target.c#L21-L26)

## Overview
BaseBackupTargetType is a structure that defines a type of backup target for PostgreSQL base backups, encapsulating the name and associated function pointers for validation and sink creation operations.

## Definition


## Detailed Description
The BaseBackupTargetType structure serves as a template or configuration object that defines how different types of backup targets should behave in PostgreSQL's base backup system. Each target type has a unique name and provides two key function pointers: one for validating target-specific details and another for creating the appropriate backup sink. This design allows the backup system to support multiple target types (like server destinations, blackhole targets, etc.) through a common interface while maintaining type-specific behavior.

## Parameters / Member Variables
- : A string identifier for the backup target type (e.g., "server", "blackhole")
- : Function pointer that validates target-specific configuration details, taking two character pointers as input and returning a void pointer to processed details
- : Function pointer that creates and returns a bbsink object for the backup target, taking a bbsink parameter and detail arguments

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (backup sink structure)
- Called from (representative examples):
  - [BaseBackupTargetHandle](BaseBackupTargetHandle.md)
  - [BaseBackupAddTarget](BaseBackupAddTarget.md)
  - [BaseBackupGetTargetHandle](BaseBackupGetTargetHandle.md)
  - [initialize_target_list](../i/initialize_target_list.md)

## Notes and Other Information
This structure is part of PostgreSQL's modular base backup system architecture, allowing different backup destinations to be supported through a plugin-like mechanism. The target types are typically registered during system initialization and used throughout the backup process to handle target-specific operations.