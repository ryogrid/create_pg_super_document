# RowIdentityVarInfo

## Location
src/include/nodes/pathnodes.h: 3036 - 3046

## Overview
RowIdentityVarInfo is a data structure that tracks row-identity "resjunk" columns in UPDATE/DELETE/MERGE operations, particularly for partitioned tables where sharing identity columns across child partitions is important to optimize targetlist usage.

## Definition

```c
typedef struct RowIdentityVarInfo
{
	pg_node_attr(no_copy_equal, no_read, no_query_jumble)

	NodeTag		type;

	Var		   *rowidvar;		/* Var to be evaluated (but varno=ROWID_VAR) */
	int32		rowidwidth;		/* estimated average width */
	char	   *rowidname;		/* name of the resjunk column */
	Relids		rowidrels;		/* RTE indexes of target rels using this */
} RowIdentityVarInfo;
```
## Detailed Description
This structure is used during query planning for UPDATE/DELETE/MERGE operations to manage row-identity columns efficiently. In partitioned tables, it's crucial to share row-identity columns whenever possible to avoid consuming too many targetlist columns. Each RowIdentityVarInfo will eventually generate one resjunk entry in the targetlist of the ModifyTable's subplan node.

The structure uses a special convention where all Vars stored must have varno ROWID_VAR for easy duplicate detection. In the final plan, this gets replaced with the actual varno of the generating relation. References to these identity variables use varno ROWID_VAR with varattno k to refer to the k-th element in the row_identity_vars list.

## Parameters / Member Variables
- : Standard NodeTag for node type identification
- : The Var expression to be evaluated, with varno set to ROWID_VAR as a placeholder
- : Estimated average width in bytes of this row identity column
- : String name assigned to this resjunk column in the targetlist  
- : Bitmap of RTE (Range Table Entry) indexes indicating which target relations use this particular row identity variable

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node type system)
  - Var (variable expression node)
  - Relids (relation ID bitmap)

- Called from (representative examples):
  - adjust_appendrel_attrs_mutator (in appendinfo.c:372)
  - add_row_identity_var (in appendinfo.c:794, 837, 855) 
  - build_joinrel_tlist (in relnode.c:1189)

## Notes and Other Information
- Uses pg_node_attr with no_copy_equal, no_read, no_query_jumble attributes to control node behavior during copying and comparison operations
- Critical for efficient handling of partitioned table modifications by avoiding duplicate row identity columns
- The ROWID_VAR convention allows the system to defer final varno assignment until plan finalization
- Part of PostgreSQL's query planning infrastructure specifically for handling complex table modification scenarios