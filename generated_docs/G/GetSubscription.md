# GetSubscription

## Location
[src/backend/catalog/pg_subscription.c:41-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L41-L122)

## Overview
Fetches a subscription record from the pg_subscription system catalog cache and constructs a Subscription structure with all subscription details.

## Definition

```c
Subscription *
GetSubscription(Oid subid, bool missing_ok)
```
## Detailed Description
GetSubscription retrieves a subscription record by its OID from the PostgreSQL system catalog cache. It performs a cache lookup using SearchSysCache1 and constructs a complete Subscription structure containing all subscription properties. The function handles both mandatory and optional subscription attributes, including connection information, slot names, publications, and various configuration flags. If the subscription is not found and missing_ok is false, it raises an ERROR; otherwise, it returns NULL.

## Parameters / Member Variables
- `subid`: The OID (Object Identifier) of the subscription to retrieve
- `missing_ok`: If true, return NULL when subscription not found; if false, raise ERROR
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
  - [superuser_arg](../s/superuser_arg.md) (check if user is superuser)
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

## Simplified Source

```c
Subscription *
GetSubscription(Oid subid, bool missing_ok)
{
    HeapTuple tup;
    Subscription *sub;
    Form_pg_subscription subform;
    Datum datum;
    bool isnull;

    // Look up subscription in system cache
    tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if (!HeapTupleIsValid(tup)) {
        if (missing_ok)
            return NULL;
        elog(ERROR, "cache lookup failed for subscription %u", subid);
    }

    subform = (Form_pg_subscription) GETSTRUCT(tup);

    // Allocate and populate Subscription structure
    sub = (Subscription *) palloc(sizeof(Subscription));
    sub->oid = subid;
    sub->dbid = subform->subdbid;
    sub->skiplsn = subform->subskiplsn;
    sub->name = pstrdup(NameStr(subform->subname));
    sub->owner = subform->subowner;
    sub->enabled = subform->subenabled;
    sub->binary = subform->subbinary;
    sub->stream = subform->substream;
    sub->twophasestate = subform->subtwophasestate;
    sub->disableonerr = subform->subdisableonerr;
    sub->passwordrequired = subform->subpasswordrequired;
    sub->runasowner = subform->subrunasowner;
    sub->failover = subform->subfailover;

    // Get connection info (required field)
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subconninfo);
    sub->conninfo = TextDatumGetCString(datum);

    // Get slot name (optional field)
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subslotname, &isnull);
    if (!isnull)
        sub->slotname = pstrdup(NameStr(*DatumGetName(datum)));
    else
        sub->slotname = NULL;

    // Get synchronous commit setting
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subsynccommit);
    sub->synccommit = TextDatumGetCString(datum);

    // Get publications list
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subpublications);
    sub->publications = textarray_to_stringlist(DatumGetArrayTypeP(datum));

    // Get origin setting
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_suborigin);
    sub->origin = TextDatumGetCString(datum);

    // Check if subscription owner is superuser
    sub->ownersuperuser = superuser_arg(sub->owner);

    ReleaseSysCache(tup);
    return sub;
}
```