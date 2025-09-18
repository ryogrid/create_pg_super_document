# BeginReportingGUCOptions

## Location
src/backend/utils/misc/guc.c: 2548 - 2597

## Overview
BeginReportingGUCOptions initializes automatic reporting of PostgreSQL configuration variables marked with GUC_REPORT flag to the frontend client upon completion of backend startup.

## Definition
void BeginReportingGUCOptions(void)

## Detailed Description
This function enables automatic reporting of configuration parameters to interactive frontends when their values change. It's called once during backend startup to establish initial communication of important GUC (Grand Unified Configuration) parameters to the client.

The function performs several key operations:
1. Checks if the output destination is an interactive frontend (DestRemote)
2. Enables the global reporting mechanism by setting reporting_enabled to true
3. Handles a special case for the in_hot_standby parameter during recovery
4. Iterates through the GUC hash table to send initial values of all variables flagged with GUC_REPORT

The function includes a specific hack for the in_hot_standby parameter, setting it to "true" when the system is in recovery mode, ensuring clients are immediately aware of the standby status.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md): Checks if database is in recovery mode
  - [SetConfigOption](../S/SetConfigOption.md): Sets configuration option value
  - [hash_seq_init](../h/hash_seq_init.md): Initializes hash table sequence scan
  - [hash_seq_search](../h/hash_seq_search.md): Searches next entry in hash sequence
  - [ReportGUCOption](../R/ReportGUCOption.md): Reports individual GUC option to frontend
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md): Called during backend initialization in src/backend/tcop/postgres.c:4345

## Notes and Other Information
- Only operates when whereToSendOutput equals DestRemote (interactive frontend)
- Uses the global guc_hashtab hash table to iterate through all configuration variables
- The in_hot_standby handling is explicitly noted as a "hack" in the comments, indicating this may not be the ideal location for this logic
- Sets the global reporting_enabled flag which controls whether subsequent configuration changes trigger automatic reports
- Essential for PostgreSQL protocol compliance as clients expect to receive GUC parameter updates