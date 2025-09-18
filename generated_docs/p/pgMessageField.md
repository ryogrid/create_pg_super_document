# pgMessageField

## Location
src/interfaces/libpq/libpq-int.h: 145 - 149

## Overview
A linked list structure used to store individual fields from PostgreSQL error and notice messages in libpq.

## Definition


## Detailed Description
The `pgMessageField` structure represents a single field within a PostgreSQL error or notice message. PostgreSQL messages are composed of multiple fields, each identified by a single-character code and containing a null-terminated string value. This structure forms a linked list to store all fields of a message.

The structure uses a flexible array member for the contents, allowing it to store variable-length field values efficiently. Each field has a specific meaning based on its code (e.g., 'M' for message text, 'S' for severity, 'C' for SQLSTATE code).

## Parameters / Member Variables
- `next`: Pointer to the next message field in the linked list
- `code`: Single character identifying the type of field (e.g., 'M', 'S', 'C')
- `contents`: Variable-length array containing the null-terminated field value

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array member support)
- Called from (representative examples):
  - Used in `pg_result` structure as `errFields` member
  - Allocated and manipulated in `fe-exec.c` for message processing

## Notes and Other Information
- This structure is part of libpq's internal message handling system
- The flexible array member allows efficient storage of variable-length field contents
- Memory allocation typically uses `offsetof(PGMessageField, contents)` to calculate the required size
- Each PostgreSQL message field has a specific single-character code that identifies its purpose
- Forms a singly-linked list with the `next` pointer to store all fields of a message
- The typedef creates the alias `PGMessageField` which is used throughout the libpq codebase