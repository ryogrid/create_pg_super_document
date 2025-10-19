# pgoutput_pubctx_reset_callback

## Location
[src/backend/replication/pgoutput/pgoutput.c:425-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L425-L433)

## Overview
A static callback function responsible for cleaning up the global publication context when the memory context is reset.

## Definition
```c
static void pgoutput_pubctx_reset_callback(void *arg)
```

## Detailed Description
This is a memory context reset callback function that ensures the global `pubctx` variable is properly cleared when the associated memory context is reset or destroyed. The function serves as a cleanup mechanism to prevent dangling pointers to publication context data that may become invalid after a memory context reset. It is registered with PostgreSQL's memory management system to be automatically called during context cleanup operations, ensuring that the pgoutput plugin maintains memory safety by nullifying references to potentially freed publication context structures.

## Parameters / Member Variables
- `arg`: Generic void pointer argument (unused in this implementation, following standard callback signature)

## Dependencies
- Functions called/Symbols referenced:
  - pubctx (global publication context variable - set to NULL)
- Called from:
  - [pgoutput_startup](pgoutput_startup.md) (registers this function as a memory context callback)

## Notes and Other Information
- Simple but critical function for memory management safety
- Prevents use-after-free errors by clearing the global pubctx pointer
- Follows PostgreSQL's memory context callback pattern
- The arg parameter is unused but required by the callback interface
- Part of the pgoutput plugin's resource management strategy
- Automatically invoked by PostgreSQL's memory management system during context cleanup

## Simplified Source

```c
static void pgoutput_pubctx_reset_callback(void *arg)
{
    // Clear global publication context pointer to prevent dangling reference
    pubctx = NULL;
}
```