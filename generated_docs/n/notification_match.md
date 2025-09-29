# notification_match

## Location
[src/backend/commands/async.c:2371-2386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L2371-L2386)

## Overview
A comparison function used by PostgreSQL's notification hash table to determine if two Notification structures are equivalent based on their channel names and payload data.

## Definition
```c
static int notification_match(const void *key1, const void *key2, Size keysize)
```

## Detailed Description
This function serves as the comparison function for the notification hash table in PostgreSQL's asynchronous notification system. It compares two Notification structures to determine if they represent the same notification event (same channel and same payload).

The comparison is performed in multiple steps for efficiency: first it checks if the channel lengths are equal, then if the payload lengths are equal, and finally performs a byte-by-byte comparison of the actual data. The data comparison includes both the channel name, payload content, and separator/terminator characters (hence the +2 in the memcmp length calculation).

This function follows the hash table convention where returning 0 indicates the keys are equal (match found) and returning 1 indicates they are not equal (no match).

## Parameters / Member Variables
- `key1`: A pointer to the first Notification structure pointer for comparison
- `key2`: A pointer to the second Notification structure pointer for comparison  
- `keysize`: The size of the key (expected to be sizeof(Notification *))

## Dependencies
- Functions called/Symbols referenced:
  - memcmp (for byte-by-byte comparison of notification data)
  - [Notification](../N/Notification.md) structure (for accessing channel_len, payload_len, and data fields)
- Called from (representative examples):
  - [AddEventToPendingNotifies](../A/AddEventToPendingNotifies.md) (as match function parameter for hash table creation)

## Notes and Other Information
- This is a static function internal to async.c
- The function expects keys to be pointers to Notification pointers (double indirection)
- Returns 0 for equal notifications, 1 for non-equal (following hash table conventions)
- Performs efficient early exit by checking lengths before doing expensive memory comparison
- The +2 in memcmp length accounts for separator/terminator characters in the data structure
- Uses assertion to verify keysize matches expected Notification pointer size

## Simplified Source

```c
static int
notification_match(const void *key1, const void *key2, Size keysize)
{
    // Extract notifications from double pointers
    const Notification *n1 = *(const Notification *const *) key1;
    const Notification *n2 = *(const Notification *const *) key2;

    // Verify expected key size
    Assert(keysize == sizeof(Notification *));

    // Quick length comparison first
    if (n1->channel_len != n2->channel_len || n1->payload_len != n2->payload_len) {
        return 1;  // Not equal
    }

    // Compare actual data (channel + payload + separators)
    if (memcmp(n1->data, n2->data, n1->channel_len + n1->payload_len + 2) == 0) {
        return 0;  // Equal
    }

    return 1;      // Not equal
}
```