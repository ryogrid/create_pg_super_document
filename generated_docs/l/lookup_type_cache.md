# lookup_type_cache

## Location
[src/backend/utils/cache/typcache.c:346-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L346-L879)

## Overview
The central function for accessing PostgreSQL's type cache, retrieving or creating TypeCacheEntry structures with the requested type information populated according to specified flags.

## Definition

```c
structures live in CacheMemoryContext,
	 * which is not quite right (they're really in the hash table's private
	 * memory context) but this will do for our purposes.
	 *
	 * Note: the code above avoids invalidating the finfo structs unless the
	 * referenced operator/function OID actually changes.  This is to prevent
	 * unnecessary leakage of any subsidiary data attached to an finfo, since
	 * that would cause session-lifespan memory leaks.
	 */
	if ((flags & TYPECACHE_EQ_OPR_FINFO) &&
		typentry->eq_opr_finfo.fn_oid == InvalidOid &&
		typentry->eq_opr != InvalidOid)
	{
		Oid			eq_opr_func;

		eq_opr_func = get_opcode(typentry->eq_opr);
		if (eq_opr_func != InvalidOid)
			fmgr_info_cxt(eq_opr_func, &typentry->eq_opr_finfo,
						  CacheMemoryContext);
	}
	if ((flags & TYPECACHE_CMP_PROC_FINFO) &&
		typentry->cmp_proc_finfo.fn_oid == InvalidOid &&
		typentry->cmp_proc != InvalidOid)
	{
		fmgr_info_cxt(typentry->cmp_proc, &typentry->cmp_proc_finfo,
					  CacheMemoryContext);
	}
	if ((flags & TYPECACHE_HASH_PROC_FINFO) &&
		typentry->hash_proc_finfo.fn_oid == InvalidOid &&
		typentry->hash_proc != InvalidOid)
	{
		fmgr_info_cxt(typentry->hash_proc, &typentry->hash_proc_finfo,
					  CacheMemoryContext);
	}
	if ((flags & TYPECACHE_HASH_EXTENDED_PROC_FINFO) &&
		typentry->hash_extended_proc_finfo.fn_oid == InvalidOid &&
		typentry->hash_extended_proc != InvalidOid)
	{
		fmgr_info_cxt(typentry->hash_extended_proc,
					  &typentry->hash_extended_proc_finfo,
					  CacheMemoryContext);
	}

	/*
	 * If it's a composite type (row type), get tupdesc if requested
	 */
	if ((flags & TYPECACHE_TUPDESC) &&
		typentry->tupDesc == NULL &&
		typentry->typtype == TYPTYPE_COMPOSITE)
	{
		load_typcache_tupdesc(typentry);
	}

	/*
	 * If requested, get information about a range type
	 *
	 * This includes making sure that the basic info about the range element
	 * type is up-to-date.
	 */
	if ((flags & TYPECACHE_RANGE_INFO) &&
		typentry->typtype == TYPTYPE_RANGE)
	{
		if (typentry->rngelemtype == NULL)
			load_rangetype_info(typentry);
		else if (!(typentry->rngelemtype->flags & TCFLAGS_HAVE_PG_TYPE_DATA))
			(void) lookup_type_cache(typentry->rngelemtype->type_id, 0);
	}

	/*
	 * If requested, get information about a multirange type
	 */
	if ((flags & TYPECACHE_MULTIRANGE_INFO) &&
		typentry->rngtype == NULL &&
		typentry->typtype == TYPTYPE_MULTIRANGE)
	{
		load_multirangetype_info(typentry);
	}

	/*
	 * If requested, get information about a domain type
	 */
	if ((flags & TYPECACHE_DOMAIN_BASE_INFO) &&
		typentry->domainBaseType == InvalidOid &&
		typentry->typtype == TYPTYPE_DOMAIN)
	{
		typentry->domainBaseTypmod = -1;
		typentry->domainBaseType =
			getBaseTypeAndTypmod(type_id, &typentry->domainBaseTypmod);
	}
	if ((flags & TYPECACHE_DOMAIN_CONSTR_INFO) &&
		(typentry->flags & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS) == 0 &&
		typentry->typtype == TYPTYPE_DOMAIN)
	{
		load_domaintype_info(typentry);
	}

	return typentry;
```
## Detailed Description
This function serves as the primary interface to PostgreSQL's type caching system. It retrieves cached type information for a given datatype OID, ensuring that all fields requested by the flags parameter are populated and up-to-date. The function manages a global hash table (TypeCacheHash) that stores TypeCacheEntry structures containing comprehensive metadata about each type.

On first access to a type, the function creates a new cache entry by consulting the pg_type system catalog and populating basic type properties. It then conditionally loads additional information based on the requested flags, such as operator class information, comparison/hashing functions, tuple descriptors for composite types, and range/domain type specifics.

The function implements sophisticated lazy loading - it only computes expensive type information when specifically requested and marks what has been loaded to avoid redundant work. It also handles cache invalidation scenarios where cached data becomes stale due to system catalog changes.

## Parameters
- `type_id`: The OID of the datatype to look up in the cache
- `flags`: Bitmask specifying which type information should be populated (e.g., TYPECACHE_EQ_OPR, TYPECACHE_CMP_PROC, TYPECACHE_TUPDESC)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md), hash_search (for hash table management)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (for pg_type catalog access)
  - [GetDefaultOpClass](../G/GetDefaultOpClass.md), get_opfamily_member, get_opfamily_proc (for operator class lookups)
  - [load_typcache_tupdesc](load_typcache_tupdesc.md), load_rangetype_info, load_multirangetype_info (for specialized type loading)
  - Various cache callback registration functions
- Called from (representative examples):
  - [array_eq](../a/array_eq.md), record_cmp, hash_range (type-specific functions needing cache info)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (expression initialization)
  - [get_sort_group_operators](../g/get_sort_group_operators.md) (query planning)

## Notes and Other Information
- Never returns NULL - will throw an error for invalid type OIDs
- Initializes the global type cache hash table on first use
- Registers invalidation callbacks to handle catalog changes
- Uses lazy evaluation to minimize overhead for unused type features  
- Thread-safe through PostgreSQL's general caching infrastructure
- The flags parameter allows fine-grained control over what information is loaded
- Special handling for composite types, domains, ranges, and multiranges
- Maintains consistency between equality operators and hash functions