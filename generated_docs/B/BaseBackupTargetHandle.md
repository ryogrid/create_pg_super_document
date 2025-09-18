# BaseBackupTargetHandle

## Location
src/backend/backup/basebackup_target.c: 28 - 60

## Overview
BaseBackupTargetHandle is a structure that represents an instance of a specific backup target, containing a reference to its target type definition and associated configuration details.

## Definition


## Detailed Description
The BaseBackupTargetHandle structure serves as a concrete instance of a backup target in PostgreSQL's base backup system. It acts as a handle or reference that combines a target type definition (BaseBackupTargetType) with specific configuration details for that particular target instance. This design separates the target type definition (which is shared across instances) from the instance-specific configuration, allowing for efficient memory usage and flexible target management. The handle is used throughout the backup process to access both the target's behavior (through the type pointer) and its specific configuration (through the detail_arg).

## Parameters / Member Variables
- : Pointer to a BaseBackupTargetType structure that defines the behavior and capabilities of this target type
- : Void pointer to target-specific configuration details or arguments, the structure of which depends on the target type

## Dependencies
- Functions called/Symbols referenced:
  - BaseBackupTargetType (target type definition)
  - initialize_target_list
  - blackhole_get_sink
  - bbsink
  - server_get_sink
  - reject_target_detail
  - server_check_detail
- Called from (representative examples):
  - BaseBackupAddTarget
  - BaseBackupGetTargetHandle
  - BaseBackupGetSink

## Notes and Other Information
This structure is part of the handle-based design pattern used in PostgreSQL's backup system, where handles provide a level of indirection between the backup logic and specific target implementations. The detail_arg member allows each target type to store custom configuration data while maintaining a uniform interface. The handle is typically created during target registration and used throughout the backup process to manage target-specific operations.