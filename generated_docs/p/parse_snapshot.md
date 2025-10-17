# parse_snapshot

## Location
[src/backend/utils/adt/xid8funcs.c:265-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L265-L333)

## Overview
A static function that parses a string representation of a PostgreSQL snapshot into a pg_snapshot structure.

## Definition
```c
static pg_snapshot *parse_snapshot(const char *str, Node *escontext)
```

## Detailed Description
This function converts a textual snapshot representation (in the format "xmin:xmax:active_xid1,active_xid2,...") into a pg_snapshot structure. It validates the format and ordering requirements, ensuring xmin and xmax are valid, xmin precedes xmax, and all active transaction IDs are properly ordered between xmin and xmax. The function uses helper functions buf_init, buf_add_txid, and buf_finalize to construct the snapshot efficiently, and includes duplicate detection to avoid redundant entries.

## Parameters / Member Variables
- `str`: String representation of the snapshot to parse (format: "xmin:xmax:active_xid1,active_xid2,...")
- `escontext`: Error context node for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionIdFromU64](../F/FullTransactionIdFromU64.md)
  - strtou64
  - FullTransactionIdIsValid
  - FullTransactionIdPrecedes
  - FullTransactionIdFollowsOrEquals
  - FullTransactionIdEquals
  - [buf_init](../b/buf_init.md)
  - [buf_add_txid](../b/buf_add_txid.md)
  - [buf_finalize](../b/buf_finalize.md)
  - ereturn
- Types referenced:
  - [FullTransactionId](../F/FullTransactionId.md)
  - InvalidFullTransactionId
  - [pg_snapshot](pg_snapshot.md)
  - [Node](../N/Node.md)
  - StringInfo
- Called from (representative examples):
  - [pg_snapshot_in](pg_snapshot_in.md)

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/xid8funcs.c
- The function expects input in the specific format "xmin:xmax:active_xid1,active_xid2,..."
- Active transaction IDs must be sorted in ascending order and fall between xmin and xmax
- Duplicate transaction IDs are automatically filtered out
- Uses soft error handling through the escontext parameter
- Returns NULL on parse error with appropriate error message
- The snapshot string format follows PostgreSQL's internal snapshot representation

## Simplified Source

```c
static pg_snapshot *parse_snapshot(const char *str, Node *escontext) {
    FullTransactionId xmin, xmax, last_val = InvalidFullTransactionId, val;
    const char *str_start = str;
    char *endp;

    // Parse xmin (format: "xmin:xmax:active_xids...")
    xmin = FullTransactionIdFromU64(strtou64(str, &endp, 10));
    if (*endp != ':') goto bad_format;
    str = endp + 1;

    // Parse xmax
    xmax = FullTransactionIdFromU64(strtou64(str, &endp, 10));
    if (*endp != ':') goto bad_format;
    str = endp + 1;

    // Validate transaction ID range
    if (!FullTransactionIdIsValid(xmin) || !FullTransactionIdIsValid(xmax) ||
        FullTransactionIdPrecedes(xmax, xmin))
        goto bad_format;

    // Initialize snapshot buffer
    StringInfo buf = buf_init(xmin, xmax);

    // Parse active transaction IDs
    while (*str != '\0') {
        val = FullTransactionIdFromU64(strtou64(str, &endp, 10));
        str = endp;

        // Validate ordering and range
        if (FullTransactionIdPrecedes(val, xmin) ||
            FullTransactionIdFollowsOrEquals(val, xmax) ||
            FullTransactionIdPrecedes(val, last_val))
            goto bad_format;

        // Add non-duplicate transaction IDs
        if (!FullTransactionIdEquals(val, last_val))
            buf_add_txid(buf, val);
        last_val = val;

        // Handle comma separator or end of string
        if (*str == ',') str++;
        else if (*str != '\0') goto bad_format;
    }

    return buf_finalize(buf);

bad_format:
    ereturn(escontext, NULL,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type %s: \"%s\"",
                    "pg_snapshot", str_start)));
}
```