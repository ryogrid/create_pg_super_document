# SendCopyEnd

## Location
src/backend/commands/copyto.c: 150 - 168

## Overview
SendCopyEnd is a static function that terminates the COPY TO protocol by sending a CopyDone message to the frontend client, signaling the completion of data transfer.

## Definition
```c
static void SendCopyEnd(CopyToState cstate)
```

## Detailed Description
This function is responsible for properly terminating a frontend copy-out operation by sending the CopyDone protocol message to the client. It includes an assertion to ensure that no data remains unsent in the message buffer before sending the completion signal. The function serves as the counterpart to SendCopyBegin, bookending the COPY TO data transfer process and informing the client that all data has been transmitted successfully.

## Parameters / Member Variables
- `cstate`: Pointer to CopyToState structure containing the state information for the copy operation, including the frontend message buffer that should be empty before completion

## Dependencies
- Functions called/Symbols referenced:
  - pq_putemptymessage (to send the empty CopyDone message)
  - PqMsg_CopyDone (message type constant for copy completion)
  - Assert (to verify no unsent data remains)
- Called from (representative examples):
  - DR_copy (in copyto.c:119)
  - DoCopyTo (in copyto.c:898)

## Notes and Other Information
- The function includes an assertion that ensures the frontend message buffer is empty before sending the done message
- This is a safety check to prevent data loss or protocol violations
- The CopyDone message is an empty message with no additional payload
- This function is static, meaning it's only accessible within the copyto.c file
- It must be called after all data has been sent via CopySendData or related functions