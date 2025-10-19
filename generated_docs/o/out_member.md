# out_member

## Location
[src/backend/access/rmgrdesc/mxactdesc.c:20-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/mxactdesc.c#L20-L49)

## Overview
A static helper function that formats a MultiXact member (transaction ID and status) into a human-readable string representation for debugging and logging purposes.

## Definition
```c
static void out_member(StringInfo buf, MultiXactMember *member)
```

## Detailed Description
The `out_member` function appends a formatted representation of a MultiXact member to a StringInfo buffer. It outputs the transaction ID followed by a status abbreviation in parentheses. This function is used internally by the multixact WAL record description system to provide readable output for debugging multixact operations.

The function handles all known MultiXact status values and provides a fallback "(unk)" for unknown status codes. Each status is represented by a short abbreviation that indicates the type of lock or operation associated with the transaction.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted output will be appended
- `member`: Pointer to a MultiXactMember structure containing the transaction ID and status to format

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [MultiXactMember](../M/MultiXactMember.md) (struct)
  - MultiXactStatusForKeyShare
  - MultiXactStatusForShare
  - MultiXactStatusForNoKeyUpdate
  - MultiXactStatusForUpdate
  - MultiXactStatusNoKeyUpdate
  - MultiXactStatusUpdate
- Called from (representative examples):
  - [multixact_desc](../m/multixact_desc.md)

## Notes and Other Information
- This is a static function, only accessible within the mxactdesc.c file
- Status abbreviations: keysh (key share), sh (share), fornokeyupd (for no key update), forupd (for update), nokeyupd (no key update), upd (update)
- Unknown status codes are handled gracefully with "(unk)" output
- Part of the WAL record description system for multixact operations

## Simplified Source

```c
static void out_member(StringInfo buf, MultiXactMember *member) {
    // Output transaction ID
    appendStringInfo(buf, "%u ", member->xid);

    // Output status abbreviation based on member status
    switch (member->status) {
        case MultiXactStatusForKeyShare:
            appendStringInfoString(buf, "(keysh) ");
            break;
        case MultiXactStatusForShare:
            appendStringInfoString(buf, "(sh) ");
            break;
        case MultiXactStatusForNoKeyUpdate:
            appendStringInfoString(buf, "(fornokeyupd) ");
            break;
        case MultiXactStatusForUpdate:
            appendStringInfoString(buf, "(forupd) ");
            break;
        case MultiXactStatusNoKeyUpdate:
            appendStringInfoString(buf, "(nokeyupd) ");
            break;
        case MultiXactStatusUpdate:
            appendStringInfoString(buf, "(upd) ");
            break;
        default:
            appendStringInfoString(buf, "(unk) ");
            break;
    }
}
```