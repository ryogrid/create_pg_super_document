# setupDumpWorker

## Location
[src/bin/pg_dump/pg_dump.c:1382-1396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1382-L1396)

## Overview
Initializes a database connection for a parallel dump worker process, ensuring it uses the same connection settings as the leader process.

## Definition


## Detailed Description
The setupDumpWorker function is a specialized connection setup routine for parallel dump worker processes. It ensures that worker processes inherit the same connection configuration as the leader process by reusing values stored in the Archive handle. The function converts the inherited encoding value back to a string format and delegates the actual connection setup to the setup_connection function. This approach maintains consistency across all parallel dump processes while avoiding duplicate configuration logic.

The function is designed specifically for the parallel dumping architecture where a leader process spawns multiple worker processes, each requiring its own database connection with identical settings for consistent data extraction.

## Parameters / Member Variables
- : Archive handle containing inherited connection configuration from the leader process, including encoding, snapshot ID, and role information

## Dependencies
- Functions called/Symbols referenced:
  - [setup_connection](setup_connection.md) (performs the actual connection configuration)
  - pg_encoding_to_char (converts encoding ID back to string representation)
- Called from (representative examples):
  - [main](../m/main.md) (when setting up parallel dump workers)
  - [CreateArchive](../C/CreateArchive.md) (during parallel worker initialization)

## Notes and Other Information
- Function is marked static, limiting scope to pg_dump.c file
- Specifically designed for parallel dump worker processes
- Inherits critical values from leader process: AH->sync_snapshot_id, AH->use_role, AH->encoding
- Passes NULL for dumpsnapshot and use_role parameters since these are already stored in the Archive handle
- Converts numeric encoding back to string format for compatibility with setup_connection
- Much simpler than the main setup_connection as it reuses leader's configuration
- Part of PostgreSQL's parallel dump architecture introduced for performance optimization