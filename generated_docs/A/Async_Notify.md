# Async_Notify

## Location
[src/backend/commands/async.c:591-689](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L591-L689)

## Overview
Executes the SQL NOTIFY command by adding notification messages to the list of pending notifications, with actual notification delivery occurring during transaction commit.

## Definition

```c
struct the Notification entry, even if we end up not using
	 * it, in order to compare it cheaply to existing list entries.
	 *
	 * The notification list needs to live until end of transaction, so store
	 * it in the transaction context.
	 */
	oldcontext = MemoryContextSwitchTo(CurTransactionContext);
```
## Detailed Description
Async_Notify is the core function that implements PostgreSQL's asynchronous notification system. When a SQL NOTIFY command is executed, this function validates the input parameters, creates a Notification structure, and adds it to the pending notifications list. The function manages notifications hierarchically across transaction nesting levels, ensuring proper cleanup and delivery semantics. It enforces length limits on channel names and payload data, prevents duplicate notifications, and handles memory context switching to ensure notifications persist until transaction end.

## Parameters / Member Variables
- : The notification channel name (must be non-empty and less than NAMEDATALEN characters)
- : Optional message payload (must be less than NOTIFY_PAYLOAD_MAX_LENGTH characters, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - IsParallelWorker
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [AsyncExistsPendingNotify](AsyncExistsPendingNotify.md)
  - [AddEventToPendingNotifies](AddEventToPendingNotifies.md)
  - [Notification](../N/Notification.md) (struct)
  - [NotificationList](../N/NotificationList.md) (struct)
- Called from (representative examples):
  - [pg_notify](../p/pg_notify.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Prevents execution from parallel workers due to shared memory constraints
- Uses CurTransactionContext for notification storage to ensure proper lifetime
- Implements deduplication to prevent duplicate notifications
- Creates hierarchical notification lists based on transaction nesting levels
- Validates channel name length against NAMEDATALEN limit
- Validates payload length against NOTIFY_PAYLOAD_MAX_LENGTH limit
- Actual notification delivery is deferred until transaction commit

## Simplified Source

```c
void Async_Notify(const char *channel, const char *payload) {
    int my_level = GetCurrentTransactionNestLevel();
    size_t channel_len = channel ? strlen(channel) : 0;
    size_t payload_len = payload ? strlen(payload) : 0;

    // Validation checks
    if (IsParallelWorker())
        elog(ERROR, "cannot send notifications from a parallel worker");

    if (channel_len == 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("channel name cannot be empty")));

    if (channel_len >= NAMEDATALEN)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("channel name too long")));

    if (payload_len >= NOTIFY_PAYLOAD_MAX_LENGTH)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("payload string too long")));

    // Create notification entry in transaction context
    MemoryContext oldcontext = MemoryContextSwitchTo(CurTransactionContext);

    Notification *n = palloc(offsetof(Notification, data) +
                            channel_len + payload_len + 2);
    n->channel_len = channel_len;
    n->payload_len = payload_len;
    strcpy(n->data, channel);

    if (payload)
        strcpy(n->data + channel_len + 1, payload);
    else
        n->data[channel_len + 1] = '\0';

    // Add to pending notifications list
    if (pendingNotifies == NULL || my_level > pendingNotifies->nestingLevel) {
        // First notify in current transaction level
        NotificationList *notifies = MemoryContextAlloc(TopTransactionContext,
                                                       sizeof(NotificationList));
        notifies->nestingLevel = my_level;
        notifies->events = list_make1(n);
        notifies->hashtab = NULL;
        notifies->upper = pendingNotifies;
        pendingNotifies = notifies;
    } else {
        // Check for duplicates before adding
        if (AsyncExistsPendingNotify(n)) {
            pfree(n);
            MemoryContextSwitchTo(oldcontext);
            return;
        }
        AddEventToPendingNotifies(n);
    }

    MemoryContextSwitchTo(oldcontext);
}
```