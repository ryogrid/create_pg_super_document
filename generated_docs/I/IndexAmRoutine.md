# IndexAmRoutine

## Location
[src/include/access/amapi.h:214-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/amapi.h#L214-L296)

## Overview
IndexAmRoutine is the main API structure that defines the interface and capabilities of an index access method (AM) in PostgreSQL, containing function pointers and capability flags that describe how the access method behaves.

## Definition

```c
typedef struct IndexAmRoutine
{
	NodeTag		type;

	/*
	 * Total number of strategies (operators) by which we can traverse/search
	 * this AM.  Zero if AM does not have a fixed set of strategy assignments.
	 */
	uint16		amstrategies;
	/* total number of support functions that this AM uses */
	uint16		amsupport;
	/* opclass options support function number or 0 */
	uint16		amoptsprocnum;
	/* does AM support ORDER BY indexed column's value? */
	bool		amcanorder;
	/* does AM support ORDER BY result of an operator on indexed column? */
	bool		amcanorderbyop;
	/* does AM support backward scanning? */
	bool		amcanbackward;
	/* does AM support UNIQUE indexes? */
	bool		amcanunique;
	/* does AM support multi-column indexes? */
	bool		amcanmulticol;
	/* does AM require scans to have a constraint on the first index column? */
	bool		amoptionalkey;
	/* does AM handle ScalarArrayOpExpr quals? */
	bool		amsearcharray;
	/* does AM handle IS NULL/IS NOT NULL quals? */
	bool		amsearchnulls;
	/* can index storage data type differ from column data type? */
	bool		amstorage;
	/* can an index of this type be clustered on? */
	bool		amclusterable;
	/* does AM handle predicate locks? */
	bool		ampredlocks;
	/* does AM support parallel scan? */
	bool		amcanparallel;
	/* does AM support parallel build? */
	bool		amcanbuildparallel;
	/* does AM support columns included with clause INCLUDE? */
	bool		amcaninclude;
	/* does AM use maintenance_work_mem? */
	bool		amusemaintenanceworkmem;
	/* does AM store tuple information only at block granularity? */
	bool		amsummarizing;
	/* OR of parallel vacuum flags.  See vacuum.h for flags. */
	uint8		amparallelvacuumoptions;
	/* type of data stored in index, or InvalidOid if variable */
	Oid			amkeytype;

	/*
	 * If you add new properties to either the above or the below lists, then
	 * they should also (usually) be exposed via the property API (see
	 * IndexAMProperty at the top of the file, and utils/adt/amutils.c).
	 */

	/* interface functions */
	ambuild_function ambuild;
	ambuildempty_function ambuildempty;
	aminsert_function aminsert;
	aminsertcleanup_function aminsertcleanup;	/* can be NULL */
	ambulkdelete_function ambulkdelete;
	amvacuumcleanup_function amvacuumcleanup;
	amcanreturn_function amcanreturn;	/* can be NULL */
	amcostestimate_function amcostestimate;
	amoptions_function amoptions;
	amproperty_function amproperty; /* can be NULL */
	ambuildphasename_function ambuildphasename; /* can be NULL */
	amvalidate_function amvalidate;
	amadjustmembers_function amadjustmembers;	/* can be NULL */
	ambeginscan_function ambeginscan;
	amrescan_function amrescan;
	amgettuple_function amgettuple; /* can be NULL */
	amgetbitmap_function amgetbitmap;	/* can be NULL */
	amendscan_function amendscan;
	ammarkpos_function ammarkpos;	/* can be NULL */
	amrestrpos_function amrestrpos; /* can be NULL */

	/* interface functions to support parallel index scans */
	amestimateparallelscan_function amestimateparallelscan; /* can be NULL */
	aminitparallelscan_function aminitparallelscan; /* can be NULL */
	amparallelrescan_function amparallelrescan; /* can be NULL */
} IndexAmRoutine;
```
## Detailed Description
IndexAmRoutine serves as the complete interface definition for index access methods in PostgreSQL. It combines capability declarations (through boolean flags and numeric properties) with function pointers that implement the actual access method operations. This structure must be stored in a single palloc'd chunk of memory and is returned by each access method's handler function.

The structure is divided into three main sections: capability properties that declare what the AM supports, boolean flags that enable/disable specific features, and function pointers that implement the AM's operations. Many function pointers are optional and can be NULL if the functionality is not supported.

Access methods use this structure to advertise their capabilities to the PostgreSQL query planner and executor, allowing the system to make informed decisions about index usage, scan strategies, and optimization opportunities.

## Parameters / Member Variables
### Capability Properties:
- : NodeTag for memory management and type checking
- : Total number of operator strategies supported by this AM (0 if no fixed set)
- : Total number of support functions used by this AM
- : Support function number for opclass options, or 0 if not supported
- : Bitwise OR of parallel vacuum capability flags
- : OID of data type stored in index, or InvalidOid if variable

### Boolean Capability Flags:
- : Can produce ordered results by indexed column values
- : Can produce ordered results by applying operators to indexed columns
- : Supports backward (descending) index scans
- : Can enforce UNIQUE constraints
- : Supports multi-column indexes
- : Requires scan constraint on first index column (false means optional)
- : Can handle ScalarArrayOpExpr (IN/ANY) qualifiers
- : Can handle IS NULL/IS NOT NULL qualifiers
- : Allows index storage type to differ from indexed column type
- : Can be used for CLUSTER operations
- : Handles predicate locks for serializable isolation
- : Supports parallel index scans
- : Supports parallel index building
- : Supports INCLUDE (non-key) columns
- : Uses maintenance_work_mem during operations
- : Stores tuple information only at block-level granularity

### Interface Function Pointers:
- Core index lifecycle: , , , 
- Maintenance operations: , 
- Query interface: , 
- Configuration: , , 
- Validation: , 
- Scan operations: , , , , 
- Cursor operations: , 
- Parallel operations: , , 

## Dependencies
- Functions called/Symbols referenced:
  - [amvalidate](../a/amvalidate.md) (validation function pointer)
- Called from (representative examples):
  - [brinhandler](../b/brinhandler.md) (BRIN access method)
  - [ginhandler](../g/ginhandler.md) (GIN access method)
  - [gisthandler](../g/gisthandler.md) (GiST access method)
  - [hashhandler](../h/hashhandler.md) (Hash access method)
  - [bthandler](../b/bthandler.md) (B-tree access method)
  - [spghandler](../s/spghandler.md) (SP-GiST access method)
  - [GetIndexAmRoutine](../G/GetIndexAmRoutine.md) (access method retrieval)
  - [DefineIndex](../D/DefineIndex.md) (index creation)
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md) (index compatibility checking)

## Notes and Other Information
- This structure is the central contract between access methods and PostgreSQL core
- Each access method must provide a handler function that returns a filled IndexAmRoutine structure
- The structure must be allocated in a single memory chunk for proper memory management
- Optional function pointers should be set to NULL if the functionality is not implemented
- Properties added to this structure should typically also be exposed via the IndexAMProperty API
- The structure supports both traditional tuple-at-a-time retrieval () and bitmap index scans ()
- Parallel operations are optional and newer additions to the API
- The capability flags are used by the query planner to determine valid execution strategies
- Different access methods implement subsets of the available functionality based on their design and intended use cases