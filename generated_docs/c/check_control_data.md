# check_control_data

## Location
src/bin/pg_upgrade/controldata.c: 654 - 710

## Overview
Validates compatibility between old and new PostgreSQL clusters by comparing critical control data settings that must match for a successful upgrade.

## Definition


## Detailed Description
The  function performs essential compatibility validation between source and target PostgreSQL clusters during pg_upgrade operations. It systematically compares fundamental database configuration parameters that cannot differ between clusters for upgrade compatibility.

The function validates multiple critical parameters:
- Memory alignment settings (detecting 32-bit vs 64-bit architecture mismatches)
- Storage block sizes (database blocks, WAL blocks, relation segments)
- Data type configurations (identifiers, indexes, TOAST chunks, large objects)
- Date/time storage format compatibility
- Data checksum version consistency

Each validation failure results in an immediate fatal error with descriptive messaging, preventing potentially dangerous upgrades that could lead to data corruption or system instability.

Special handling includes:
- Large object chunk size validation (only for PostgreSQL 9.5+)
- Data checksum version restrictions (preventing checksum format mismatches)
- Float8 pass-by-value checking is noted but handled separately in other functions

## Parameters / Member Variables
- : ControlData structure containing control information from the source cluster
- : ControlData structure containing control information from the target cluster

## Dependencies
- Functions called/Symbols referenced:
  - [pg_fatal](../p/pg_fatal.md) (error handling and termination)
  - ControlData (data structure for control information)
- Called from (representative examples):
  - [check_cluster_compatibility](check_cluster_compatibility.md) (src/bin/pg_upgrade/check.c:844)

## Notes and Other Information
- Terminates pg_upgrade immediately upon any compatibility violation with detailed error messages
- Ensures critical database parameters match between clusters to prevent data corruption during upgrade
- Some parameters like float8_pass_by_value are validated separately in specialized functions
- Data checksum compatibility is strictly enforced - clusters must both use checksums or both not use them
- The function acts as a critical safety gate in the upgrade process, preventing potentially catastrophic mismatched upgrades