# PGMessageField

## Location
[src/interfaces/libpq/libpq-int.h:150-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-int.h#L150-L158)

## Overview
A typedef alias for the `pgMessageField` structure, used to store individual fields from PostgreSQL error and notice messages in libpq.

## Definition

```c
typedef struct
{
	PQnoticeReceiver noticeRec; /* notice message receiver */
	void	   *noticeRecArg;
	PQnoticeProcessor noticeProc;	/* notice message processor */
	void	   *noticeProcArg;
} PGNoticeHooks;
```
## Detailed Description
`PGMessageField` is the typedef name for the `pgMessageField` structure. This alias follows PostgreSQL's naming convention where internal structures often have both a lowercase struct name and a capitalized typedef alias. The structure represents a single field within a PostgreSQL error or notice message and forms part of a linked list to store all message fields.

PostgreSQL protocol messages contain multiple fields, each identified by a single-character code. This structure efficiently stores these fields using a flexible array member for variable-length content.

## Parameters / Member Variables
- `next`: Pointer to the next message field in the linked list
- `code`: Single character identifying the field type (e.g., 'M' for message, 'S' for severity)
- `contents`: Variable-length array containing the null-terminated field value

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array member support)
  - [pgMessageField](../p/pgMessageField.md) (the underlying struct type)
- Called from (representative examples):
  - Used in `pg_result` structure as `errFields` member
  - Referenced in `fe-exec.c` for message field processing and allocation

## Notes and Other Information
- This is the preferred typedef name used throughout the libpq codebase
- The structure implements a singly-linked list for storing message fields
- Memory allocation accounts for the flexible array member using `offsetof(PGMessageField, contents)`
- Common field codes include 'S' (severity), 'C' (SQLSTATE), 'M' (message), 'D' (detail), 'H' (hint)
- Part of libpq's internal error and notice message handling infrastructure
- The flexible array member allows efficient storage without separate memory allocation for field contents