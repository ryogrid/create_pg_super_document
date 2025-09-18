# SYSCACHECALLBACK

## Location
src/backend/utils/cache/inval.c: 256 - 267

## Overview
SYSCACHECALLBACK is a structure that defines an entry in the system cache callback list, used to register callback functions that are invoked when system caches are invalidated.

## Definition


## Detailed Description
The SYSCACHECALLBACK structure is part of PostgreSQL's cache invalidation system. It maintains a static array of callback entries that are executed when system caches need to be invalidated. The structure implements a linked list organization where callbacks for the same cache are chained together using the link field. This design allows efficient traversal of callbacks for specific cache types during invalidation events. The maximum number of callbacks is limited by MAX_SYSCACHE_CALLBACKS (64).

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Cache number identifying which system cache this callback is associated with
- : Index+1 to the next callback in the linked list for the same cache, or 0 for end of list
- : Pointer to the callback function of type SyscacheCallbackFunction that will be invoked
- : Datum argument passed to the callback function when it is invoked

## Dependencies
- Functions called/Symbols referenced:
  - MAX_SYSCACHE_CALLBACKS (constant defining array size)
  - SyscacheCallbackFunction (callback function type)
- Called from (representative examples):
  - [InvalidateSystemCachesExtended](../I/InvalidateSystemCachesExtended.md) (iterates through callback list and invokes functions)
  - [CallSyscacheCallbacks](../C/CallSyscacheCallbacks.md) (processes callbacks for specific cache invalidation)

## Notes and Other Information
- The structure is declared as static, limiting its visibility to the inval.c file
- Callbacks are organized as a linked list to optimize searching during invalidation
- The implementation assumes a relatively small number of callbacks, using a fixed-size array
- Part of PostgreSQL's cache invalidation infrastructure for maintaining cache consistency
- Located in src/backend/utils/cache/inval.c:256-267