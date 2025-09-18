# pg_control_system

## Location
src/backend/utils/misc/pg_controldata.c: 32 - 69

## Overview
A PostgreSQL SQL function that retrieves and returns basic system information from the control file as a composite tuple.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL-callable function that reads the control file and returns essential system-level information as a row type. It acquires the ControlFileLock in shared mode to safely read the control file, validates the CRC checksum, and extracts four key system identifiers. The function is part of PostgreSQL's administrative interface that allows SQL queries to access control file metadata without requiring direct file system access.

## Parameters / Member Variables
- No input parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Returns a composite tuple containing:
  - : Version number of the control file format
  - : System catalog version number  
  - : Unique system identifier for the database cluster
  - : Timestamp when the control file was last updated

## Dependencies
- Functions called/Symbols referenced:
  - get_call_result_type: Validates return type
  - LWLockAcquire/LWLockRelease: Manages concurrent access to control file
  - get_controlfile: Reads and parses the control file
  - Int32GetDatum/Int64GetDatum: Converts values to PostgreSQL Datum format
  - time_t_to_timestamptz: Converts time_t to PostgreSQL timestamp
  - heap_form_tuple: Creates the return tuple
  - ControlFileData: Structure containing control file data
- Called from (representative examples):
  - SQL queries via function call mechanism

## Notes and Other Information
- Requires shared lock on ControlFileLock to ensure consistent reads
- Validates control file CRC checksum and raises ERROR if corrupted
- Part of the pg_controldata family of functions for administrative access
- Located in src/backend/utils/misc/pg_controldata.c:32-69