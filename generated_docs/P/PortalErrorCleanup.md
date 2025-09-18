# PortalErrorCleanup

## Location
src/backend/utils/mmgr/portalmem.c: 917 - 942

## Overview
PortalErrorCleanup handles cleanup of auto-held portals when returning to the main loop after an error, providing different behavior than transaction abort cleanup.

## Definition
void PortalErrorCleanup(void)

## Detailed Description
PortalErrorCleanup is a specialized cleanup function called when PostgreSQL returns to the main loop after encountering an error condition. Unlike transaction abort cleanup functions (AtAbort_Portals and AtCleanup_Portals), this function specifically targets auto-held portals for cleanup:

1. **Auto-held Portal Focus**: Only processes portals that have the autoHeld flag set, which are portals that were automatically held by the system
2. **Forced Unpin**: Removes any pin on auto-held portals before dropping them
3. **Portal Removal**: Drops auto-held portals using PortalDrop() to free their resources
4. **Error Recovery**: Ensures clean state when recovering from errors that don't trigger full transaction abort

This function provides a different cleanup strategy than transaction abort, as auto-held portals survive transaction abort but are cleaned up during error recovery to prevent resource accumulation.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - PortalDrop
  - HASH_SEQ_STATUS
  - PortalHashEnt
- Called from (representative examples):
  - [PostgresMain](PostgresMain.md)

## Notes and Other Information
- Called specifically during error recovery in the main PostgreSQL loop, not during transaction processing
- Distinguishes between error cleanup and transaction abort cleanup behavior
- Auto-held portals are preserved during transaction abort but cleaned up during error recovery
- Ensures auto-held portals don't accumulate and consume resources during repeated error conditions
- Part of PostgreSQL's error recovery mechanism to maintain system stability
- Much simpler than other portal cleanup functions due to its specific focus on auto-held portals only