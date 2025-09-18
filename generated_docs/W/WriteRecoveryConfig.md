# WriteRecoveryConfig

## Location
src/fe_utils/recovery_gen.c: 124 - 162

## Overview
WriteRecoveryConfig writes recovery configuration content to the appropriate configuration file and creates necessary signal files for PostgreSQL standby server setup.

## Definition


## Detailed Description
This function writes the recovery configuration to the filesystem, handling version-specific differences in PostgreSQL's recovery configuration mechanism. For PostgreSQL versions prior to 12, it writes to recovery.conf. For version 12 and later, it appends to postgresql.auto.conf and creates a standby.signal file to trigger standby mode.

The function determines the appropriate configuration method based on the server version, writes the provided configuration content to the correct file, and creates the standby.signal file when necessary. This ensures proper standby server initialization across different PostgreSQL versions.

## Parameters / Member Variables
- : Database connection used to determine server version
- : Directory path where configuration files should be written
- : PQExpBuffer containing the recovery configuration content to write

## Dependencies
- Functions called/Symbols referenced:
  - PQserverVersion
  - MINIMUM_VERSION_FOR_RECOVERY_GUC
  - snprintf
  - fopen
  - fwrite
  - fclose
  - pg_fatal
- Called from (representative examples):
  - setup_recovery (pg_createsubscriber.c:1237)
  - main (pg_rewind.c:453, 530)

## Notes and Other Information
- Handles PostgreSQL version differences: uses recovery.conf for pre-12 versions, postgresql.auto.conf + standby.signal for 12+
- For older versions, opens recovery.conf in write mode ('w'), completely replacing its contents
- For newer versions, opens postgresql.auto.conf in append mode ('a'), preserving existing configuration
- Creates an empty standby.signal file for PostgreSQL 12+ to trigger standby mode
- Calls pg_fatal() on file operation failures
- The function assumes the target directory already exists and is writable