# RELCACHECALLBACK

## Location
[src/backend/utils/cache/inval.c:268-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L268-L290)

## Overview
RELCACHECALLBACK is a structure that defines an entry in the relation cache callback list, used to register callback functions that are invoked when relation cache entries are invalidated.

## Definition

```c
struct RELCACHECALLBACK
{
	RelcacheCallbackFunction function;
	Datum		arg;
}			relcache_callback_list[MAX_RELCACHE_CALLBACKS];
```
## Detailed Description
The RELCACHECALLBACK structure is part of PostgreSQL's relation cache invalidation system. It maintains a static array of callback entries that are executed when relation cache entries need to be invalidated. Unlike SYSCACHECALLBACK, this structure is simpler and doesn't use a linked list organization since relation cache callbacks are processed sequentially. The structure stores callback functions that are invoked when specific relations are invalidated or when the entire relation cache is flushed. The maximum number of callbacks is limited by MAX_RELCACHE_CALLBACKS (10).

## Parameters / Member Variables
- `function`: Pointer to the callback function of type RelcacheCallbackFunction that will be invoked during relation cache invalidation
- `arg`: Datum argument passed to the callback function when it is invoked

## Dependencies
- Functions called/Symbols referenced:
  - MAX_RELCACHE_CALLBACKS (constant defining array size, set to 10)
  - RelcacheCallbackFunction (callback function type)
- Called from (representative examples):
  - [InvalidateSystemCachesExtended](../I/InvalidateSystemCachesExtended.md) (iterates through callback list and invokes functions)
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md) (processes callbacks for relation cache invalidation)

## Notes and Other Information
- The structure is declared as static, limiting its visibility to the inval.c file
- Simpler than SYSCACHECALLBACK as it doesn't require linked list organization
- Has a smaller maximum capacity (10) compared to system cache callbacks (64)
- Called when relation cache entries are invalidated, either for specific relations or entire cache
- Part of PostgreSQL's cache invalidation infrastructure for maintaining relation cache consistency
- Located in src/backend/utils/cache/inval.c:268-272