# getNotify

## Location
[src/interfaces/libpq/fe-protocol3.c:1498-1553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1498-L1553)

## Overview
Processes asynchronous notification messages from the PostgreSQL server, creating PGnotify structures and adding them to the connection's notification queue.

## Definition

```c
structure so it can all be
	 * freed at once.  We don't use NAMEDATALEN because we don't want to tie
	 * this interface to a specific server name length.
	 */
	nmlen = strlen(svname);
```
## Detailed Description
This static function handles Notify messages ('A') sent by the PostgreSQL server when a LISTEN/NOTIFY event occurs. The message contains the backend process ID that sent the notification, the channel name (relation name), and optional payload data. The function creates a PGnotify structure to store this information and adds it to the connection's notification queue for later retrieval by the application.

The function allocates memory for the entire notification structure in a single block, with the strings stored immediately after the PGnotify structure. This design allows the entire notification to be freed with a single free() call. The notifications are maintained in a linked list using the connection's notifyHead and notifyTail pointers.

## Parameters / Member Variables
- : PostgreSQL connection object containing the notification queue and buffers for reading message data

## Dependencies
- Functions called/Symbols referenced:
  - [pqGetInt](../p/pqGetInt.md)
  - [pqGets](../p/pqGets.md)
  - strdup
  - free
  - strlen
  - malloc
  - strcpy
- Called from (representative examples):
  - [pqParseInput3](../p/pqParseInput3.md)
  - [getCopyDataMessage](getCopyDataMessage.md)
  - [pqFunctionCall3](../p/pqFunctionCall3.md)

## Notes and Other Information
- Returns 0 on successful message consumption, EOF if insufficient data available or memory allocation failure
- Function is declared static, limiting its visibility to the fe-protocol3.c file  
- Message format: backend PID (4 bytes), channel name (null-terminated string), payload (null-terminated string)
- Memory layout: PGnotify structure followed immediately by channel name and payload strings
- Notifications are queued in FIFO order using notifyHead/notifyTail linked list
- Channel name length is not restricted to NAMEDATALEN to avoid server version dependencies
- Handles memory allocation failure gracefully by simply not queuing the notification
- Applications retrieve notifications using PQnotifies() function
- Entry assumes 'A' message type and length have already been consumed
- Temporary copy of channel name is needed since workBuffer is reused for payload reading

## Simplified Source

```c
static int
getNotify(PGconn *conn)
{
    int backend_pid;
    char *channel_name;
    int name_len, payload_len;
    PGnotify *newNotify;

    // Read backend process ID
    if (pqGetInt(&backend_pid, 4, conn))
        return EOF;

    // Read channel name
    if (pqGets(&conn->workBuffer, conn))
        return EOF;

    // Save channel name (workBuffer will be reused for payload)
    channel_name = strdup(conn->workBuffer.data);
    if (!channel_name)
        return EOF;

    // Read payload data
    if (pqGets(&conn->workBuffer, conn))
    {
        free(channel_name);
        return EOF;
    }

    // Calculate memory needed for notification structure + strings
    name_len = strlen(channel_name);
    payload_len = strlen(conn->workBuffer.data);
    newNotify = (PGnotify *) malloc(sizeof(PGnotify) + name_len + payload_len + 2);

    if (newNotify)
    {
        // Set up string storage immediately after the PGnotify structure
        newNotify->relname = (char *) newNotify + sizeof(PGnotify);
        strcpy(newNotify->relname, channel_name);

        newNotify->extra = newNotify->relname + name_len + 1;
        strcpy(newNotify->extra, conn->workBuffer.data);

        newNotify->be_pid = backend_pid;
        newNotify->next = NULL;

        // Add to notification queue (FIFO order)
        if (conn->notifyTail)
            conn->notifyTail->next = newNotify;
        else
            conn->notifyHead = newNotify;
        conn->notifyTail = newNotify;
    }

    free(channel_name);
    return 0;
}
```