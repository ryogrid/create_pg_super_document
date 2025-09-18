# libpqrcv_readtimelinehistoryfile

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 732 - 785

## Overview
Fetches the timeline history file for a specified timeline ID from the primary server during WAL replication.

## Definition


## Detailed Description
This function sends a TIMELINE_HISTORY command to the primary server to retrieve the timeline history file for a given timeline ID. The timeline history file contains information about timeline switches and is crucial for WAL replication consistency. The function validates the response format and extracts both the filename and content of the history file.

The function uses the libpq protocol to send the command and expects exactly one tuple with two fields in response: the filename and the binary content of the timeline history file.

## Parameters / Member Variables
- : WAL receiver connection object containing the stream connection to the primary
- : Timeline ID for which to fetch the history file
- : Output parameter that receives a palloc'd copy of the history filename
- : Output parameter that receives a palloc'd copy of the file content
- : Output parameter that receives the length of the content in bytes

## Dependencies
- Functions called/Symbols referenced:
  - libpqrcv_PQexec (for sending the TIMELINE_HISTORY command)
  - PQresultStatus (for checking command result status)
  - PQnfields (for validating response structure)
  - PQntuples (for validating response structure) 
  - PQgetvalue (for extracting result data)
  - PQgetlength (for getting content length)
  - PQclear (for cleaning up result)
  - pstrdup (for copying filename)
  - palloc (for allocating content buffer)
  - pchomp (for error message formatting)
- Called from (representative examples):
  - Used internally by WAL receiver functions during timeline history retrieval

## Notes and Other Information
- This is a static function, only accessible within libpqwalreceiver.c
- The function asserts that the connection is not for logical replication (conn->logical must be false)
- Error handling includes specific protocol violation errors for malformed responses
- Memory allocation for both filename and content is done using PostgreSQL's memory management functions
- The function expects exactly 1 tuple with 2 fields, otherwise it raises a protocol violation error