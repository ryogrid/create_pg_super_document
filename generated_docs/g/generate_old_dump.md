# generate_old_dump

## Location
src/bin/pg_upgrade/dump.c: 16 - 71

## Overview
Generates database dumps from the old PostgreSQL cluster during pg_upgrade operations, creating both global objects dump and individual database schema dumps.

## Definition


## Detailed Description
The  function is a critical component of PostgreSQL's pg_upgrade utility that creates comprehensive dumps of the old cluster's data structure before performing the upgrade. The function operates in two main phases:

1. **Global Objects Phase**: Creates a dump of cluster-wide objects (roles, tablespaces, databases, etc.) using pg_dumpall with the  option.

2. **Database Schema Phase**: Iterates through all databases in the old cluster and creates individual schema-only dumps using pg_dump with binary upgrade format. These dumps are executed in parallel for better performance.

The function uses binary upgrade mode () which preserves PostgreSQL internal identifiers (OIDs) that are crucial for maintaining object relationships during the upgrade process. All dumps are saved to the configured dump directory with standardized filenames.

## Parameters / Member Variables
This function takes no parameters as it operates on global cluster state variables:
- Uses  global variable to access source cluster information
- Uses  to locate the new PostgreSQL binaries
- Uses  for logging configuration and dump directory location

## Dependencies
- Functions called/Symbols referenced:
  -  - Initialize status reporting for global objects dump
  -  - Execute pg_dumpall for global objects
  -  - Generate connection options for old cluster
  -  - Verify successful completion of operations
  -  - Initialize progress reporting for database schemas
  -  - Execute pg_dump commands in parallel for each database
  - , , , ,  - String buffer operations for connection strings
  -  - Log database processing status
  -  - Wait for parallel processes to complete
  -  - Finalize progress reporting

- Called from (representative examples):
  -  - Main upgrade orchestration function

## Notes and Other Information
- The function assumes the new cluster binaries (pg_dump, pg_dumpall) are compatible with the old cluster data
- Uses custom format dumps () for database schemas, which provides better performance and compression
- Implements parallel processing for database dumps to improve performance on multi-database clusters  
- Escapes connection strings properly to handle database names with special characters
- All dump files are created with standardized naming conventions using masks like 
- The  option ensures that all database object names are properly quoted to handle reserved words and special characters
- Binary upgrade mode preserves critical metadata that standard dumps would not include, such as OID assignments