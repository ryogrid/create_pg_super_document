# pqSaveMessageField

## Location
src/interfaces/libpq/fe-exec.c: 1060 - 1080

## Overview
pqSaveMessageField is a utility function that saves one field of an error or notice message into a PGresult structure, managing the linked list of message fields.

## Definition


## Detailed Description
This function creates and stores a message field in the PGresult's error/notice field linked list. It allocates memory for a PGMessageField structure that includes both the field metadata and the field content string in a single allocation. The function uses pqResultAlloc() to ensure the memory is properly tracked and will be freed when the PGresult is destroyed.

The function implements a singly-linked list where new fields are prepended to the existing list (res->errFields), making the most recently added field the head of the list. Each field consists of a code (identifying the field type) and the actual text content.

## Parameters / Member Variables
- : Pointer to the PGresult structure that will store the message field
- : Single character code identifying the field type (e.g., 'M' for primary message, 'S' for severity)
- : String content of the message field to be stored

## Dependencies
- Functions called/Symbols referenced:
  - [pqResultAlloc](pqResultAlloc.md) (for memory allocation with result tracking)
  - strcpy (for copying the field value)
  - strlen (implicit, used in size calculation)
  - offsetof (for calculating structure size)
- Types used:
  - [PGMessageField](../P/PGMessageField.md) (structure representing a single message field)
- Called from:
  - [pqInternalNotice](pqInternalNotice.md) (multiple times for different field types)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md) (for processing server error/notice messages)

## Notes and Other Information
- Uses a variable-length allocation strategy: allocates exactly the space needed for the PGMessageField structure plus the string content
- The allocation includes space for the null terminator of the string
- Memory allocation is performed with the 'true' parameter to pqResultAlloc(), indicating the allocation should be tracked
- On memory allocation failure, the function silently returns without adding the field
- Fields are stored in reverse order of addition (newest first) due to prepending to the linked list
- The function is designed to handle both error messages and notice messages
- Field codes follow PostgreSQL's diagnostic field conventions (e.g., PG_DIAG_MESSAGE_PRIMARY, PG_DIAG_SEVERITY)
- Memory is automatically managed through the PGresult's memory context and freed when PQclear() is called