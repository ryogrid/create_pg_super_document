# testcustomrmgrs_desc

## Location
src/test/modules/test_custom_rmgrs/test_custom_rmgrs.c: 91 - 105

## Overview
Formats a human-readable description of test custom resource manager WAL records for debugging and logging purposes.

## Definition


## Detailed Description
The  function implements the description callback for the test_custom_rmgrs custom WAL resource manager. This function is part of the RMGR API and is called to generate human-readable descriptions of WAL records for debugging tools, log analysis, and WAL examination utilities like .

The function extracts the operation type from the WAL record and, if it matches , formats the record's payload information into the provided StringInfo buffer. It displays both the size of the message payload and the actual message content.

The function casts the raw WAL record data to the expected  structure and appends both textual and binary information about the message payload to the output buffer using PostgreSQL's StringInfo formatting functions.

## Parameters / Member Variables
- : StringInfo buffer where the formatted description will be appended
- : Pointer to an  structure containing the WAL record to be described

## Dependencies
- Functions called/Symbols referenced:
  -  (extracts raw record data)
  -  (extracts record info/flags)
  -  (constant for masking info flags)
  -  (expected operation code constant)
  -  (record structure type)
  -  (formats text into StringInfo buffer)
  -  (appends binary data to StringInfo buffer)
- Called from (representative examples):
  -  utility for WAL record examination
  - PostgreSQL logging and debugging systems
  - Custom resource manager framework via  structure

## Notes and Other Information
- This function is primarily used for debugging and introspection of WAL records
- The output format shows both payload size and actual message content for easy analysis
- Only processes  type records, silently ignoring others
- Located in 
- Part of the test custom resource manager registered with ID 
- The binary data display allows examination of the actual message payload stored in the WAL record