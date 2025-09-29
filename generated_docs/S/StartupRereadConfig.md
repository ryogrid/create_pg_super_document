# StartupRereadConfig

## Location
[src/backend/postmaster/startup.c:125-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L125-L153)

## Overview
Reloads the PostgreSQL configuration file and restarts the WAL receiver if critical walreceiver options have changed during recovery operations.

## Definition
static void StartupRereadConfig(void)

## Detailed Description
StartupRereadConfig handles the configuration reloading process specifically for the startup process during recovery operations. When called (typically in response to a SIGHUP signal), it first preserves the current values of critical WAL receiver parameters (PrimaryConnInfo, PrimarySlotName, and wal_receiver_create_temp_slot), then processes the configuration file with PGC_SIGHUP context.

After reloading, the function compares the new configuration values with the previously stored ones to determine if any critical WAL receiver settings have changed. If changes are detected in connection information, slot name, or temporary slot creation settings, it triggers a WAL receiver restart by calling StartupRequestWalReceiverRestart(). The function includes special logic for the wal_receiver_create_temp_slot setting, which is only relevant when no specific slot is configured.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ProcessConfigFile (reloads configuration with PGC_SIGHUP context)
  - [StartupRequestWalReceiverRestart](StartupRequestWalReceiverRestart.md) (requests WAL receiver restart)
  - [pstrdup](../p/pstrdup.md) (duplicates strings for comparison)
  - [pfree](../p/pfree.md) (frees allocated memory)
  - strcmp (compares strings)
  - PGC_SIGHUP (configuration context constant)
- Called from (representative examples):
  - [HandleStartupProcInterrupts](../H/HandleStartupProcInterrupts.md) (processes configuration reload requests)

## Notes and Other Information
- Called in response to SIGHUP signals during recovery operations
- Preserves original configuration values to detect changes effectively
- Only restarts WAL receiver when critical settings actually change, avoiding unnecessary disruptions
- Handles special case logic for wal_receiver_create_temp_slot when no slot is configured
- Part of PostgreSQL's dynamic configuration management during recovery
- Memory management includes proper cleanup of duplicated strings
- Ensures configuration changes take effect without interrupting the recovery process unnecessarily

## Simplified Source

```c
static void
StartupRereadConfig(void)
{
	// Save current WAL receiver settings
	char *conninfo = pstrdup(PrimaryConnInfo);
	char *slotname = pstrdup(PrimarySlotName);
	bool tempSlot = wal_receiver_create_temp_slot;
	bool conninfoChanged;
	bool slotnameChanged;
	bool tempSlotChanged = false;

	// Reload configuration
	ProcessConfigFile(PGC_SIGHUP);

	// Check for changes in critical settings
	conninfoChanged = strcmp(conninfo, PrimaryConnInfo) != 0;
	slotnameChanged = strcmp(slotname, PrimarySlotName) != 0;

	// Check temp slot change only if no slot configured
	if (!slotnameChanged && strcmp(PrimarySlotName, "") == 0)
		tempSlotChanged = tempSlot != wal_receiver_create_temp_slot;

	pfree(conninfo);
	pfree(slotname);

	// Restart WAL receiver if any critical setting changed
	if (conninfoChanged || slotnameChanged || tempSlotChanged)
		StartupRequestWalReceiverRestart();
}
```