# GetSubscription

## Location
src/backend/catalog/pg_subscription.c: 41 - 122

## Overview
Fetches a subscription record from the pg_subscription system catalog cache and constructs a Subscription structure with all subscription details.

## Definition


## Detailed Description
GetSubscription retrieves a subscription record by its OID from the PostgreSQL system catalog cache. It performs a cache lookup using SearchSysCache1 and constructs a complete Subscription structure containing all subscription properties. The function handles both mandatory and optional subscription attributes, including connection information, slot names, publications, and various configuration flags. If the subscription is not found and missing_ok is false, it raises an ERROR; otherwise, it returns NULL.

## Parameters / Member Variables
- : The OID (Object Identifier) of the subscription to retrieve
- : If true, return NULL when subscription not found; if false, raise ERROR

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (extract form data from tuple)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md) (get non-null attributes)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (get potentially null attributes)
  - TextDatumGetCString (convert text datum to C string)
  - [DatumGetName](../D/DatumGetName.md) (extract name from datum)
  - [textarray_to_stringlist](../t/textarray_to_stringlist.md) (convert text array to string list)
  - DatumGetArrayTypeP (convert datum to array type)
  - superuser_arg (check if user is superuser)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release cache entry)
- Called from (representative examples):
  - [AlterSubscription](../A/AlterSubscription.md) (subscription management)
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md) (logical replication worker)
  - [InitializeLogRepWorker](../I/InitializeLogRepWorker.md) (logical replication initialization)

## Notes and Other Information
- Uses PostgreSQL's system cache (syscache) for efficient repeated access to subscription data
- Constructs a complete Subscription structure with all fields populated from the pg_subscription catalog
- Handles optional fields like slotname which may be NULL
- Determines ownership privileges by checking if the subscription owner is a superuser
- Memory allocated for the Subscription structure and string fields should be freed by the caller
- Part of PostgreSQL's logical replication subscription management system