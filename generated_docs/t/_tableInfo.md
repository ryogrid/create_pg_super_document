# _tableInfo

## Location
[src/bin/pg_dump/pg_dump.h:295-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L295-L378)

## Overview
The  structure is a comprehensive data structure used by pg_dump to store metadata information about database tables during the dump process.

## Definition

```c
typedef struct _tableInfo
{
	/*
	 * These fields are collected for every table in the database.
	 */
	DumpableObject dobj;
	DumpableAcl dacl;
	const char *rolname;
	char		relkind;
	char		relpersistence; /* relation persistence */
	bool		relispopulated; /* relation is populated */
	char		relreplident;	/* replica identifier */
	char	   *reltablespace;	/* relation tablespace */
	char	   *reloptions;		/* options specified by WITH (...) */
	char	   *checkoption;	/* WITH CHECK OPTION, if any */
	char	   *toast_reloptions;	/* WITH options for the TOAST table */
	bool		hasindex;		/* does it have any indexes? */
	bool		hasrules;		/* does it have any rules? */
	bool		hastriggers;	/* does it have any triggers? */
	bool		hascolumnACLs;	/* do any columns have non-default ACLs? */
	bool		rowsec;			/* is row security enabled? */
	bool		forcerowsec;	/* is row security forced? */
	bool		hasoids;		/* does it have OIDs? */
	uint32		frozenxid;		/* table's relfrozenxid */
	uint32		minmxid;		/* table's relminmxid */
	Oid			toast_oid;		/* toast table's OID, or 0 if none */
	uint32		toast_frozenxid;	/* toast table's relfrozenxid, if any */
	uint32		toast_minmxid;	/* toast table's relminmxid */
	int			ncheck;			/* # of CHECK expressions */
	Oid			reltype;		/* OID of table's composite type, if any */
	Oid			reloftype;		/* underlying type for typed table */
	Oid			foreign_server; /* foreign server oid, if applicable */
	/* these two are set only if table is a sequence owned by a column: */
	Oid			owning_tab;		/* OID of table owning sequence */
	int			owning_col;		/* attr # of column owning sequence */
	bool		is_identity_sequence;
	int			relpages;		/* table's size in pages (from pg_class) */
	int			toastpages;		/* toast table's size in pages, if any */

	bool		interesting;	/* true if need to collect more data */
	bool		dummy_view;		/* view's real definition must be postponed */
	bool		postponed_def;	/* matview must be postponed into post-data */
	bool		ispartition;	/* is table a partition? */
	bool		unsafe_partitions;	/* is it an unsafe partitioned table? */

	int			numParents;		/* number of (immediate) parent tables */
	struct _tableInfo **parents;	/* TableInfos of immediate parents */

	/*
	 * These fields are computed only if we decide the table is interesting
	 * (it's either a table to dump, or a direct parent of a dumpable table).
	 */
	int			numatts;		/* number of attributes */
	char	  **attnames;		/* the attribute names */
	char	  **atttypnames;	/* attribute type names */
	int		   *attstattarget;	/* attribute statistics targets */
	char	   *attstorage;		/* attribute storage scheme */
	char	   *typstorage;		/* type storage scheme */
	bool	   *attisdropped;	/* true if attr is dropped; don't dump it */
	char	   *attidentity;
	char	   *attgenerated;
	int		   *attlen;			/* attribute length, used by binary_upgrade */
	char	   *attalign;		/* attribute align, used by binary_upgrade */
	bool	   *attislocal;		/* true if attr has local definition */
	char	  **attoptions;		/* per-attribute options */
	Oid		   *attcollation;	/* per-attribute collation selection */
	char	   *attcompression; /* per-attribute compression method */
	char	  **attfdwoptions;	/* per-attribute fdw options */
	char	  **attmissingval;	/* per attribute missing value */
	bool	   *notnull;		/* not-null constraints on attributes */
	bool	   *inhNotNull;		/* true if NOT NULL is inherited */
	struct _attrDefInfo **attrdefs; /* DEFAULT expressions */
	struct _constraintInfo *checkexprs; /* CHECK constraints */
	bool		needs_override; /* has GENERATED ALWAYS AS IDENTITY */
	char	   *amname;			/* relation access method */

	/*
	 * Stuff computed only for dumpable tables.
	 */
	int			numIndexes;		/* number of indexes */
	struct _indxInfo *indexes;	/* indexes */
	struct _tableDataInfo *dataObj; /* TableDataInfo, if dumping its data */
	int			numTriggers;	/* number of triggers for table */
	struct _triggerInfo *triggers;	/* array of TriggerInfo structs */
} TableInfo;
```
## Detailed Description
The  structure serves as the central repository for all metadata related to database tables in pg_dump. It contains comprehensive information about table properties, attributes, constraints, indexes, and relationships. The structure is organized into three logical sections: basic table information collected for all tables, detailed attribute information computed for interesting tables, and dump-specific data for tables that will be dumped.

## Parameters / Member Variables
### Basic Table Information
- : Base dumpable object information
- : Access control list information
- : Name of the table owner role
- : Relation kind (table, view, sequence, etc.)
- : Relation persistence (permanent, temporary, unlogged)
- : Whether the relation is populated
- : Replica identity setting
- : Tablespace name where the table resides
- : Storage options specified with WITH clause
- : WITH CHECK OPTION for views
- : Storage options for the TOAST table
- : Whether the table has any indexes
- : Whether the table has any rules
- : Whether the table has any triggers
- : Whether any columns have non-default ACLs
- : Whether row security is enabled
- : Whether row security is forced
- : Whether the table has OIDs
- : Table's relfrozenxid for VACUUM freeze tracking
- : Table's relminmxid for multixact tracking
- : OID of the associated TOAST table
- : TOAST table's relfrozenxid
- : TOAST table's relminmxid
- : Number of CHECK constraints
- : OID of table's composite type
- : Underlying type for typed tables
- : Foreign server OID for foreign tables
- : OID of table owning this sequence
- : Column number owning this sequence
- : Whether this is an identity sequence
- : Table size in pages
- : TOAST table size in pages

### Processing Control
- : Whether to collect detailed information
- : Whether view definition must be postponed
- : Whether materialized view must be postponed
- : Whether the table is a partition
- : Whether it's an unsafe partitioned table
- : Number of immediate parent tables
- : Array of immediate parent TableInfo structures

### Detailed Attribute Information
- : Number of attributes
- : Attribute names array
- : Attribute type names array
- : Statistics targets for attributes
- : Attribute storage schemes
- : Type storage schemes
- : Whether attributes are dropped
- : Identity column information
- : Generated column information
- : Attribute lengths for binary upgrade
- : Attribute alignment for binary upgrade
- : Whether attributes have local definitions
- : Per-attribute options
- : Per-attribute collation selections
- : Per-attribute compression methods
- : Per-attribute foreign data wrapper options
- : Per-attribute missing values
- : NOT NULL constraints on attributes
- : Whether NOT NULL is inherited
- : DEFAULT expressions for attributes
- : CHECK constraint expressions
- : Whether table has GENERATED ALWAYS AS IDENTITY
- : Access method name

### Dump-Specific Information
- : Number of indexes
- : Array of index information structures
- : Table data information for dumping
- : Number of triggers

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
  - [_attrDefInfo](../a/_attrDefInfo.md)
  - [_constraintInfo](../c/_constraintInfo.md)
  - [_indxInfo](../i/_indxInfo.md)
  - [_tableDataInfo](_tableDataInfo.md)
  - [_triggerInfo](_triggerInfo.md)
- Called from (representative examples):
  - Self-referential for parent table relationships

## Notes and Other Information
This structure is central to pg_dump's operation and represents the complete metadata model for database tables. The three-tier organization allows for efficient memory usage by only collecting detailed information for tables that are actually needed for the dump operation. The structure supports inheritance relationships, partitioning, foreign tables, and all PostgreSQL table features.