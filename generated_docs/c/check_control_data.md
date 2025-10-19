# check_control_data

## Location
[src/bin/pg_upgrade/controldata.c:654-710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/controldata.c#L654-L710)

## Overview
Validates compatibility between old and new PostgreSQL clusters by comparing critical control data settings that must match for a successful upgrade.

## Definition

```c
void
check_control_data(ControlData *oldctrl,
				   ControlData *newctrl)
```
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
- `*oldctrl`: ControlData structure containing control information from the source cluster
- `*newctrl`: ControlData structure containing control information from the target cluster
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

## Simplified Source

```c
void check_control_data(ControlData *oldctrl, ControlData *newctrl) {
    // Check memory alignment (32-bit vs 64-bit compatibility)
    if (oldctrl->align == 0 || oldctrl->align != newctrl->align)
        pg_fatal("old and new pg_controldata alignments are invalid or do not match.\n"
                 "Likely one cluster is a 32-bit install, the other 64-bit");

    // Check block sizes must match
    if (oldctrl->blocksz == 0 || oldctrl->blocksz != newctrl->blocksz)
        pg_fatal("old and new pg_controldata block sizes are invalid or do not match");

    // Check WAL configuration
    if (oldctrl->walsz == 0 || oldctrl->walsz != newctrl->walsz)
        pg_fatal("old and new pg_controldata WAL block sizes are invalid or do not match");

    if (oldctrl->walseg == 0 || oldctrl->walseg != newctrl->walseg)
        pg_fatal("old and new pg_controldata WAL segment sizes are invalid or do not match");

    // Check data type configurations
    if (oldctrl->ident == 0 || oldctrl->ident != newctrl->ident)
        pg_fatal("old and new pg_controldata maximum identifier lengths are invalid or do not match");

    if (oldctrl->toast == 0 || oldctrl->toast != newctrl->toast)
        pg_fatal("old and new pg_controldata maximum TOAST chunk sizes are invalid or do not match");

    // Check date/time storage compatibility
    if (oldctrl->date_is_int != newctrl->date_is_int)
        pg_fatal("old and new pg_controldata date/time storage types do not match");

    // Check data checksum compatibility
    if (oldctrl->data_checksum_version == 0 && newctrl->data_checksum_version != 0)
        pg_fatal("old cluster does not use data checksums but the new one does");
    else if (oldctrl->data_checksum_version != 0 && newctrl->data_checksum_version == 0)
        pg_fatal("old cluster uses data checksums but the new one does not");
    else if (oldctrl->data_checksum_version != newctrl->data_checksum_version)
        pg_fatal("old and new cluster pg_controldata checksum versions do not match");
}
```