# notification_hash

## Location
[src/backend/commands/async.c:2357-2370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L2357-L2370)

## Overview
A hash function used by PostgreSQL's notification hash table to generate hash values for Notification structures based on their channel name and payload data.

## Definition
```c
static uint32 notification_hash(const void *key, Size keysize)
```

## Detailed Description
This function serves as the hash function for the notification hash table used in PostgreSQL's asynchronous notification system. It takes a pointer to a Notification structure pointer as input and generates a hash value based on the notification's data content.

The function extracts the channel name and payload data from the Notification structure and uses PostgreSQL's general-purpose hash_any function to generate the hash. It specifically includes the channel name length, payload length, and one additional byte (likely for a separator or null terminator) in the hash calculation, but explicitly excludes the payload's trailing null character.

The hash is computed over the combined data of channel name and payload to ensure that notifications with different channels or payloads will have different hash values, enabling efficient duplicate detection and lookup in the hash table.

## Parameters / Member Variables
- `key`: A pointer to a Notification structure pointer (double indirection) that serves as the hash key
- `keysize`: The size of the key (expected to be sizeof(Notification *))

## Dependencies
- Functions called/Symbols referenced:
  - [hash_any](../h/hash_any.md) (PostgreSQL's general-purpose hash function)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (converts hash result to uint32)
  - [Notification](../N/Notification.md) structure (for accessing channel_len, payload_len, and data fields)
- Called from (representative examples):
  - [AddEventToPendingNotifies](../A/AddEventToPendingNotifies.md) (as hash function parameter for hash table creation)

## Notes and Other Information
- This is a static function internal to async.c
- The function expects the key to be a pointer to a Notification pointer (double indirection)
- Deliberately excludes the payload's trailing null character from the hash calculation
- Uses an assertion to verify that keysize matches the expected size of a Notification pointer
- The hash covers both channel name and payload data to ensure proper uniqueness

## Simplified Source

```c
static uint32
notification_hash(const void *key, Size keysize)
{
    // Extract notification from double pointer
    const Notification *notification = *(const Notification *const *) key;

    // Verify expected key size
    Assert(keysize == sizeof(Notification *));

    // Hash channel name + payload data (exclude trailing null)
    return DatumGetUInt32(hash_any((const unsigned char *) notification->data,
                                   notification->channel_len + notification->payload_len + 1));
}
```