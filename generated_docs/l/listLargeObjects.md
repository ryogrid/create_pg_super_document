# listLargeObjects

## Location
src/bin/psql/describe.c: 7054 - 7092

## Overview
The  function implements the  and  psql commands to display a formatted list of large objects stored in the PostgreSQL database.

## Definition


## Detailed Description
This function constructs and executes an SQL query to retrieve large object information from the PostgreSQL system catalog . It displays large objects with their OIDs (object identifiers), owners, and descriptions. In verbose mode, it additionally shows access control lists (ACLs) that define permissions for each large object. Large objects in PostgreSQL are a facility for storing binary data that can be larger than typical data types allow, and they are managed through a special API.

The query uses system functions like  to resolve owner IDs to usernames and  to retrieve descriptive comments. Results are sorted by OID for consistent presentation. The function is used by both the  describe command and the  large object command.

## Parameters / Member Variables
- : Boolean flag that controls whether to include additional columns (access control lists/ACLs) in the output. When true, shows permissions granted on each large object.

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [PSQLexec](../P/PSQLexec.md)
  - termPQExpBuffer
  - [printQuery](../p/printQuery.md)
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - [printQueryOpt](../p/printQueryOpt.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql describe command dispatcher)
  - [exec_command_lo](../e/exec_command_lo.md) (large object command handler)

## Notes and Other Information
- This function is part of psql's describe command family, specifically handling the  command and large object listing functionality
- The function uses internationalization support through gettext_noop() for column headers
- Error handling includes proper cleanup of allocated buffers on failure paths
- Large objects are stored in a special system table () with metadata in 
- Each large object has a unique OID that serves as its identifier
- Large objects support access control through ACLs, which can grant various permissions (read, write, etc.) to different users
- The function queries  rather than  because metadata provides the essential information without the actual binary data
- Large objects are often used for storing files, images, documents, or other binary data that exceeds PostgreSQL's standard data type limits
- The  function retrieves comments that can be associated with large objects using the COMMENT SQL command
- This function is simpler than other describe functions as it doesn't support pattern matching (there are typically fewer large objects to filter)