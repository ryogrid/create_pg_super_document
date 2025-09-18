# cleanup_objects_atexit

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 157 - 216

## Overview
Cleanup function that removes PostgreSQL objects (publications and replication slots) that were created by pg_createsubscriber when an error occurs during the subscription setup process.

## Definition


## Detailed Description
This function serves as an error handler registered with atexit() to perform cleanup operations when pg_createsubscriber fails. It attempts to remove publications and replication slots that were created on the primary server during the subscription setup process. The function operates differently depending on whether the target server has been promoted or not:

- If recovery has ended (server promoted), it warns the user that the physical replica cannot be reused
- For each database, it attempts to connect and drop any publications or replication slots that were created
- If connection fails, it logs warnings about objects left behind on the primary
- If the standby server is running, it stops it

The function only executes if the global  flag is false, indicating an error occurred.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- : Global flag indicating if the operation completed successfully
- : Flag indicating if recovery has ended (server promoted)
- : Number of databases being processed
- : Array of database information structures containing publication and replication slot details
- : Flag indicating if the standby server is running
- : Directory path for the subscriber

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_warning
  - pg_log_warning_hint
  - connect_database
  - drop_publication
  - drop_replication_slot
  - disconnect_database
  - stop_standby_server
- Called from (representative examples):
  - main (registered as atexit handler)

## Notes and Other Information
- This is a static function specific to pg_createsubscriber utility
- Designed as a best-effort cleanup - if connections fail, it logs warnings instead of failing
- Critical for preventing resource leaks on the primary server when subscription setup fails
- Does not attempt cleanup on the target server after promotion, as the replica would need to be recreated anyway
- Uses conditional cleanup based on flags tracking what objects were actually created during the process