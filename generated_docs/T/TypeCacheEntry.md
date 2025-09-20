# TypeCacheEntry

## Location
[src/include/utils/typcache.h:31-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/typcache.h#L31-L134)

## Overview
TypeCacheEntry is a comprehensive data structure that caches type-related information for PostgreSQL data types to avoid repeated lookups and computations during query execution.

## Definition

```c
typedef struct TypeCacheEntry
{
	/* typeId is the hash lookup key and MUST BE FIRST */
	Oid			type_id;		/* OID of the data type */

	uint32		type_id_hash;	/* hashed value of the OID */

	/* some subsidiary information copied from the pg_type row */
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typstorage;
	char		typtype;
	Oid			typrelid;
	Oid			typsubscript;
	Oid			typelem;
	Oid			typcollation;

	/*
	 * Information obtained from opfamily entries
	 *
	 * These will be InvalidOid if no match could be found, or if the
	 * information hasn't yet been requested.  Also note that for array and
	 * composite types, typcache.c checks that the contained types are
	 * comparable or hashable before allowing eq_opr etc to become set.
	 */
	Oid			btree_opf;		/* the default btree opclass' family */
	Oid			btree_opintype; /* the default btree opclass' opcintype */
	Oid			hash_opf;		/* the default hash opclass' family */
	Oid			hash_opintype;	/* the default hash opclass' opcintype */
	Oid			eq_opr;			/* the equality operator */
	Oid			lt_opr;			/* the less-than operator */
	Oid			gt_opr;			/* the greater-than operator */
	Oid			cmp_proc;		/* the btree comparison function */
	Oid			hash_proc;		/* the hash calculation function */
	Oid			hash_extended_proc; /* the extended hash calculation function */

	/*
	 * Pre-set-up fmgr call info for the equality operator, the btree
	 * comparison function, and the hash calculation function.  These are kept
	 * in the type cache to avoid problems with memory leaks in repeated calls
	 * to functions such as array_eq, array_cmp, hash_array.  There is not
	 * currently a need to maintain call info for the lt_opr or gt_opr.
	 */
	FmgrInfo	eq_opr_finfo;
	FmgrInfo	cmp_proc_finfo;
	FmgrInfo	hash_proc_finfo;
	FmgrInfo	hash_extended_proc_finfo;

	/*
	 * Tuple descriptor if it's a composite type (row type).  NULL if not
	 * composite or information hasn't yet been requested.  (NOTE: this is a
	 * reference-counted tupledesc.)
	 *
	 * To simplify caching dependent info, tupDesc_identifier is an identifier
	 * for this tupledesc that is unique for the life of the process, and
	 * changes anytime the tupledesc does.  Zero if not yet determined.
	 */
	TupleDesc	tupDesc;
	uint64		tupDesc_identifier;

	/*
	 * Fields computed when TYPECACHE_RANGE_INFO is requested.  Zeroes if not
	 * a range type or information hasn't yet been requested.  Note that
	 * rng_cmp_proc_finfo could be different from the element type's default
	 * btree comparison function.
	 */
	struct TypeCacheEntry *rngelemtype; /* range's element type */
	Oid			rng_opfamily;	/* opfamily to use for range comparisons */
	Oid			rng_collation;	/* collation for comparisons, if any */
	FmgrInfo	rng_cmp_proc_finfo; /* comparison function */
	FmgrInfo	rng_canonical_finfo;	/* canonicalization function, if any */
	FmgrInfo	rng_subdiff_finfo;	/* difference function, if any */

	/*
	 * Fields computed when TYPECACHE_MULTIRANGE_INFO is required.
	 */
	struct TypeCacheEntry *rngtype; /* multirange's range underlying type */

	/*
	 * Domain's base type and typmod if it's a domain type.  Zeroes if not
	 * domain, or if information hasn't been requested.
	 */
	Oid			domainBaseType;
	int32		domainBaseTypmod;

	/*
	 * Domain constraint data if it's a domain type.  NULL if not domain, or
	 * if domain has no constraints, or if information hasn't been requested.
	 */
	DomainConstraintCache *domainData;

	/* Private data, for internal use of typcache.c only */
	int			flags;			/* flags about what we've computed */

	/*
	 * Private information about an enum type.  NULL if not enum or
	 * information hasn't been requested.
	 */
	struct TypeCacheEnumData *enumData;

	/* We also maintain a list of all known domain-type cache entries */
	struct TypeCacheEntry *nextDomain;
} TypeCacheEntry;
```
## Detailed Description
TypeCacheEntry serves as a comprehensive caching mechanism for type-related metadata in PostgreSQL's type system. This structure eliminates the need for repeated catalog lookups and expensive function setup operations by maintaining pre-computed information about data types, their operators, and associated functions.

The cache entry is organized into several logical sections: basic type properties copied from pg_type, operator family information for comparison and hashing operations, pre-initialized function manager information for frequently used operations, and specialized data for complex types like ranges, domains, enums, and composite types.

The structure is designed with performance in mind - the type_id field must be first to serve as the hash key, and frequently accessed function information is pre-initialized to avoid repeated fmgr_info() calls during query execution.

## Parameters / Member Variables
### Core Type Information
- : The OID of the data type, serves as the primary hash lookup key
- : Pre-computed hash value of the type OID for faster lookups
- : Length of the type (-1 for variable length, -2 for cstring)
- : Whether the type is passed by value or reference
- : Alignment requirement for the type ('c', 's', 'i', 'd')
- : Storage strategy ('p'lain, 'e'xternal, 'm'ain, 'x'tended)
- : Type category ('b'ase, 'c'omposite, 'd'omain, 'e'num, 'p'seudo, 'r'ange, 'm'ultirange)
- : OID of the relation if this is a composite type
- : OID of the subscripting handler function
- : OID of the element type for arrays
- : OID of the default collation for the type

### Operator Family Information
- : Default B-tree operator class family OID
- : Input type OID for the B-tree operator class
- : Default hash operator class family OID
- : Input type OID for the hash operator class
- : OID of the equality operator
- : OID of the less-than operator
- : OID of the greater-than operator
- : OID of the B-tree comparison function
- : OID of the hash calculation function
- : OID of the extended hash calculation function

### Pre-initialized Function Information
- : Pre-setup function manager info for equality operator
- : Pre-setup function manager info for comparison function
- : Pre-setup function manager info for hash function
- : Pre-setup function manager info for extended hash function

### Composite Type Information
- : Tuple descriptor for composite types (reference-counted)
- : Unique identifier for the tuple descriptor lifetime

### Range Type Information
- : Pointer to the TypeCacheEntry of the range's element type
- : Operator family OID for range comparisons
- : Collation OID for range comparisons
- : Pre-setup comparison function info for ranges
- : Pre-setup canonicalization function info for ranges
- : Pre-setup difference function info for ranges

### Multirange Type Information
- : Pointer to the TypeCacheEntry of the underlying range type

### Domain Type Information
- : OID of the base type for domain types
- : Type modifier of the base type for domain types
- : Pointer to domain constraint cache data

### Internal Management
- : Bit flags indicating which information has been computed
- : Pointer to enum-specific cached data
- : Pointer to next domain type entry in linked list

## Dependencies
- Functions called/Symbols referenced:
  - [DomainConstraintCache](../D/DomainConstraintCache.md)
  - [TypeCacheEnumData](TypeCacheEnumData.md)
- Called from (representative examples):
  - Various functions in typcache.c
  - Type comparison and hashing operations throughout PostgreSQL

## Notes and Other Information
- The type_id field must be positioned first in the structure as it serves as the hash lookup key
- Function manager information is pre-initialized to avoid memory leaks in repeated function calls
- The tuple descriptor for composite types is reference-counted and must be managed carefully
- Domain types are maintained in a linked list via the nextDomain pointer
- The flags field uses bit patterns to track which optional information has been computed and cached
- This structure is central to PostgreSQL's type system performance, avoiding repeated catalog lookups during query execution