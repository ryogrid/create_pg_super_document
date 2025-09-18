# SubXactCallbackItem

## Location
src/backend/access/transam/xact.c: 317 - 322

## Overview
SubXactCallbackItem is a linked list node structure that manages subtransaction callback functions, enabling modules to register for start-of-subtransaction and end-of-subtransaction notifications.

## Definition


## Detailed Description
SubXactCallbackItem implements a linked list mechanism specifically designed for subtransaction lifecycle management. Similar to XactCallbackItem but focused on subtransaction events, this structure allows PostgreSQL subsystems and extensions to register callback functions that are invoked during subtransaction start, commit, and abort operations. This fine-grained callback system is essential for managing resources and state that must be coordinated with savepoint operations and nested transaction boundaries.

## Parameters / Member Variables
- : Pointer to the next callback item in the linked list
- : Function pointer to the subtransaction callback function (SubXactCallback type)
- : Generic void pointer to user-defined argument data passed to the callback

## Dependencies
- Functions called/Symbols referenced:
  - SubXactCallbackItem (self-reference for linked list)
  - callback (function pointer field)
- Called from (representative examples):
  - RegisterSubXactCallback
  - UnregisterSubXactCallback
  - CallSubXactCallbacks

## Notes and Other Information
The subtransaction callback mechanism is crucial for PostgreSQL's savepoint functionality and nested transaction support. Unlike main transaction callbacks, subtransaction callbacks must handle more complex scenarios including partial rollbacks, cascading aborts, and savepoint releases. This structure enables proper resource cleanup and state management during complex transaction nesting scenarios, making it essential for features like cursor management, temporary object cleanup, and lock management within subtransactions.