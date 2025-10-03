# report_extension_updates

## Location
[src/bin/pg_upgrade/version.c:147-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/version.c#L147-L211)

## Overview
A PostgreSQL upgrade utility function that identifies extensions requiring updates and generates an update script to bring them to their latest versions.

## Definition

```c
void
report_extension_updates(ClusterInfo *cluster)
```
## Detailed Description
This function is part of the pg_upgrade utility and is responsible for detecting PostgreSQL extensions that have newer versions available than what is currently installed. It scans all databases in the cluster to identify extensions where the installed version differs from the default (latest) version available. When such extensions are found, the function generates an "update_extensions.sql" script containing ALTER EXTENSION UPDATE commands that can be executed post-upgrade to bring all extensions to their current versions.

The function queries the pg_available_extensions view to compare installed_version with default_version for each extension, ensuring that users are made aware of available updates that should be applied after the cluster upgrade completes.

## Parameters / Member Variables
- `*cluster`: Pointer to ClusterInfo structure containing metadata about the PostgreSQL cluster being examined for extension updates
## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting initialization)
  - [connectToServer](../c/connectToServer.md) (database connection establishment)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL query execution)
  - fopen_priv (secure file creation)
  - [PQExpBufferData](../P/PQExpBufferData.md), initPQExpBuffer, appendPsqlMetaConnect, termPQExpBuffer (query buffer management)
  - [quote_identifier](../q/quote_identifier.md) (SQL identifier quoting for safety)
  - [PQclear](../P/PQclear.md), PQfinish (PostgreSQL result and connection cleanup)
  - report_status, pg_log (logging and user notification)
  - [check_ok](../c/check_ok.md) (successful completion reporting)
- Called from (representative examples):
  - [issue_warnings_and_set_wal_level](../i/issue_warnings_and_set_wal_level.md) (during upgrade process)

## Notes and Other Information
- Uses the pg_available_extensions system view to identify extension version discrepancies
- Generates a post-upgrade script rather than performing updates during the upgrade process
- Only creates the update script file if extensions needing updates are actually found
- Provides informative messages to guide users on executing the generated script
- The generated script must be executed by a database superuser after upgrade completion
- Part of pg_upgrade's comprehensive post-upgrade maintenance task identification system
- Helps ensure that extensions remain current and compatible with the new PostgreSQL version