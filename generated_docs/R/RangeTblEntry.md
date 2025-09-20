# RangeTblEntry

## Location
[src/include/nodes/parsenodes.h:1038-1251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1038-L1251)

## Overview
RangeTblEntry is a fundamental data structure in PostgreSQL that represents an entry in a range table, which contains information about each table, subquery, function, or other data source referenced in a SQL query.

## Definition

```c
typedef struct RangeTblEntry
{
	pg_node_attr(custom_read_write)

	NodeTag		type;

	/*
	 * Fields valid in all RTEs:
	 *
	 * put alias + eref first to make dump more legible
	 */
	/* user-written alias clause, if any */
	Alias	   *alias pg_node_attr(query_jumble_ignore);
	/* expanded reference names */
	Alias	   *eref pg_node_attr(query_jumble_ignore);

	RTEKind		rtekind;		/* see above */

	/*
	 * Fields valid for a plain relation RTE (else zero):
	 *
	 * inh is true for relation references that should be expanded to include
	 * inheritance children, if the rel has any.  In the parser, this will
	 * only be true for RTE_RELATION entries.  The planner also uses this
	 * field to mark RTE_SUBQUERY entries that contain UNION ALL queries that
	 * it has flattened into pulled-up subqueries (creating a structure much
	 * like the effects of inheritance).
	 *
	 * rellockmode is really LOCKMODE, but it's declared int to avoid having
	 * to include lock-related headers here.  It must be RowExclusiveLock if
	 * the RTE is an INSERT/UPDATE/DELETE/MERGE target, else RowShareLock if
	 * the RTE is a SELECT FOR UPDATE/FOR SHARE target, else AccessShareLock.
	 *
	 * Note: in some cases, rule expansion may result in RTEs that are marked
	 * with RowExclusiveLock even though they are not the target of the
	 * current query; this happens if a DO ALSO rule simply scans the original
	 * target table.  We leave such RTEs with their original lockmode so as to
	 * avoid getting an additional, lesser lock.
	 *
	 * perminfoindex is 1-based index of the RTEPermissionInfo belonging to
	 * this RTE in the containing struct's list of same; 0 if permissions need
	 * not be checked for this RTE.
	 *
	 * As a special case, relid, relkind, rellockmode, and perminfoindex can
	 * also be set (nonzero) in an RTE_SUBQUERY RTE.  This occurs when we
	 * convert an RTE_RELATION RTE naming a view into an RTE_SUBQUERY
	 * containing the view's query.  We still need to perform run-time locking
	 * and permission checks on the view, even though it's not directly used
	 * in the query anymore, and the most expedient way to do that is to
	 * retain these fields from the old state of the RTE.
	 *
	 * As a special case, RTE_NAMEDTUPLESTORE can also set relid to indicate
	 * that the tuple format of the tuplestore is the same as the referenced
	 * relation.  This allows plans referencing AFTER trigger transition
	 * tables to be invalidated if the underlying table is altered.
	 */
	/* OID of the relation */
	Oid			relid;
	/* inheritance requested? */
	bool		inh;
	/* relation kind (see pg_class.relkind) */
	char		relkind pg_node_attr(query_jumble_ignore);
	/* lock level that query requires on the rel */
	int			rellockmode pg_node_attr(query_jumble_ignore);
	/* index of RTEPermissionInfo entry, or 0 */
	Index		perminfoindex pg_node_attr(query_jumble_ignore);
	/* sampling info, or NULL */
	struct TableSampleClause *tablesample;

	/*
	 * Fields valid for a subquery RTE (else NULL):
	 */
	/* the sub-query */
	Query	   *subquery;
	/* is from security_barrier view? */
	bool		security_barrier pg_node_attr(query_jumble_ignore);

	/*
	 * Fields valid for a join RTE (else NULL/zero):
	 *
	 * joinaliasvars is a list of (usually) Vars corresponding to the columns
	 * of the join result.  An alias Var referencing column K of the join
	 * result can be replaced by the K'th element of joinaliasvars --- but to
	 * simplify the task of reverse-listing aliases correctly, we do not do
	 * that until planning time.  In detail: an element of joinaliasvars can
	 * be a Var of one of the join's input relations, or such a Var with an
	 * implicit coercion to the join's output column type, or a COALESCE
	 * expression containing the two input column Vars (possibly coerced).
	 * Elements beyond the first joinmergedcols entries are always just Vars,
	 * and are never referenced from elsewhere in the query (that is, join
	 * alias Vars are generated only for merged columns).  We keep these
	 * entries only because they're needed in expandRTE() and similar code.
	 *
	 * Vars appearing within joinaliasvars are marked with varnullingrels sets
	 * that describe the nulling effects of this join and lower ones.  This is
	 * essential for FULL JOIN cases, because the COALESCE expression only
	 * describes the semantics correctly if its inputs have been nulled by the
	 * join.  For other cases, it allows expandRTE() to generate a valid
	 * representation of the join's output without consulting additional
	 * parser state.
	 *
	 * Within a Query loaded from a stored rule, it is possible for non-merged
	 * joinaliasvars items to be null pointers, which are placeholders for
	 * (necessarily unreferenced) columns dropped since the rule was made.
	 * Also, once planning begins, joinaliasvars items can be almost anything,
	 * as a result of subquery-flattening substitutions.
	 *
	 * joinleftcols is an integer list of physical column numbers of the left
	 * join input rel that are included in the join; likewise joinrighttcols
	 * for the right join input rel.  (Which rels those are can be determined
	 * from the associated JoinExpr.)  If the join is USING/NATURAL, then the
	 * first joinmergedcols entries in each list identify the merged columns.
	 * The merged columns come first in the join output, then remaining
	 * columns of the left input, then remaining columns of the right.
	 *
	 * Note that input columns could have been dropped after creation of a
	 * stored rule, if they are not referenced in the query (in particular,
	 * merged columns could not be dropped); this is not accounted for in
	 * joinleftcols/joinrighttcols.
	 */
	JoinType	jointype;
	/* number of merged (JOIN USING) columns */
	int			joinmergedcols pg_node_attr(query_jumble_ignore);
	/* list of alias-var expansions */
	List	   *joinaliasvars pg_node_attr(query_jumble_ignore);
	/* left-side input column numbers */
	List	   *joinleftcols pg_node_attr(query_jumble_ignore);
	/* right-side input column numbers */
	List	   *joinrightcols pg_node_attr(query_jumble_ignore);

	/*
	 * join_using_alias is an alias clause attached directly to JOIN/USING. It
	 * is different from the alias field (below) in that it does not hide the
	 * range variables of the tables being joined.
	 */
	Alias	   *join_using_alias pg_node_attr(query_jumble_ignore);

	/*
	 * Fields valid for a function RTE (else NIL/zero):
	 *
	 * When funcordinality is true, the eref->colnames list includes an alias
	 * for the ordinality column.  The ordinality column is otherwise
	 * implicit, and must be accounted for "by hand" in places such as
	 * expandRTE().
	 */
	/* list of RangeTblFunction nodes */
	List	   *functions;
	/* is this called WITH ORDINALITY? */
	bool		funcordinality;

	/*
	 * Fields valid for a TableFunc RTE (else NULL):
	 */
	TableFunc  *tablefunc;

	/*
	 * Fields valid for a values RTE (else NIL):
	 */
	/* list of expression lists */
	List	   *values_lists;

	/*
	 * Fields valid for a CTE RTE (else NULL/zero):
	 */
	/* name of the WITH list item */
	char	   *ctename;
	/* number of query levels up */
	Index		ctelevelsup;
	/* is this a recursive self-reference? */
	bool		self_reference pg_node_attr(query_jumble_ignore);

	/*
	 * Fields valid for CTE, VALUES, ENR, and TableFunc RTEs (else NIL):
	 *
	 * We need these for CTE RTEs so that the types of self-referential
	 * columns are well-defined.  For VALUES RTEs, storing these explicitly
	 * saves having to re-determine the info by scanning the values_lists. For
	 * ENRs, we store the types explicitly here (we could get the information
	 * from the catalogs if 'relid' was supplied, but we'd still need these
	 * for TupleDesc-based ENRs, so we might as well always store the type
	 * info here).  For TableFuncs, these fields are redundant with data in
	 * the TableFunc node, but keeping them here allows some code sharing with
	 * the other cases.
	 *
	 * For ENRs only, we have to consider the possibility of dropped columns.
	 * A dropped column is included in these lists, but it will have zeroes in
	 * all three lists (as well as an empty-string entry in eref).  Testing
	 * for zero coltype is the standard way to detect a dropped column.
	 */
	/* OID list of column type OIDs */
	List	   *coltypes pg_node_attr(query_jumble_ignore);
	/* integer list of column typmods */
	List	   *coltypmods pg_node_attr(query_jumble_ignore);
	/* OID list of column collation OIDs */
	List	   *colcollations pg_node_attr(query_jumble_ignore);

	/*
	 * Fields valid for ENR RTEs (else NULL/zero):
	 */
	/* name of ephemeral named relation */
	char	   *enrname;
	/* estimated or actual from caller */
	Cardinality enrtuples pg_node_attr(query_jumble_ignore);

	/*
	 * Fields valid in all RTEs:
	 */
	/* was LATERAL specified? */
	bool		lateral pg_node_attr(query_jumble_ignore);
	/* present in FROM clause? */
	bool		inFromCl pg_node_attr(query_jumble_ignore);
	/* security barrier quals to apply, if any */
	List	   *securityQuals pg_node_attr(query_jumble_ignore);
} RangeTblEntry;
```
## Detailed Description
RangeTblEntry is a polymorphic structure that represents different kinds of range table entries in PostgreSQL's query processing system. The rtekind field determines which subset of fields are valid for a particular instance. This structure is central to PostgreSQL's query representation and is used throughout the parser, planner, and executor phases.

The structure supports various types of data sources including regular relations (tables/views), subqueries, joins, functions, VALUES clauses, Common Table Expressions (CTEs), Ephemeral Named Relations (ENRs), and table functions. Each type uses different subsets of the available fields.

## Parameters / Member Variables
- : NodeTag identifying this as a RangeTblEntry node
- : User-written alias clause for the range table entry
- : Expanded reference names (effective column names after alias resolution)
- : Specifies the kind of RTE (relation, subquery, join, function, etc.)
- : OID of the relation (for relation RTEs)
- : Whether inheritance should be considered for relation references
- : Kind of relation from pg_class.relkind
- : Lock level required on the relation (LOCKMODE stored as int)
- : 1-based index into RTEPermissionInfo list, 0 if no permission check needed
- : Sampling information for table sampling clauses
- : The sub-query for subquery RTEs
- : Whether this is from a security_barrier view
- : Type of join for join RTEs
- : Number of merged columns in JOIN USING
- : List of alias variable expansions for join columns
- : Physical column numbers from left join input
- : Physical column numbers from right join input
- : Alias clause attached directly to JOIN/USING
- : List of RangeTblFunction nodes for function RTEs
- : Whether function is called WITH ORDINALITY
- : TableFunc node for table function RTEs
- : List of expression lists for VALUES RTEs
- : Name of the WITH list item for CTE RTEs
- : Number of query levels up for CTE references
- : Whether this is a recursive self-reference in CTE
- : OID list of column type OIDs
- : List of column type modifiers
- : OID list of column collation OIDs
- : Name of ephemeral named relation
- : Estimated or actual tuple count for ENRs
- : Whether LATERAL was specified
- : Whether this RTE appears in the FROM clause
- : Security barrier qualifiers to apply

## Dependencies
- Functions called/Symbols referenced:
  - [Alias](../A/Alias.md)
  - [RTEKind](RTEKind.md)
  - [TableSampleClause](../T/TableSampleClause.md)
  - JoinType
  - [TableFunc](../T/TableFunc.md)
  - Cardinality
- Called from (representative examples):
  - [Query](../Q/Query.md) parser functions
  - Planner routines
  - Executor initialization

## Notes and Other Information
- The structure uses conditional compilation attributes (pg_node_attr) to control serialization and query fingerprinting behavior
- Different RTEKind values activate different subsets of fields, making this a polymorphic structure
- Lock modes are stored as integers to avoid including lock-related headers
- The structure handles complex scenarios like view expansion, join alias resolution, and CTE recursion
- Special handling exists for dropped columns in ENRs (represented as zero values)
- [Query](../Q/Query.md) jumble attributes are used to exclude certain fields from query fingerprinting for performance optimization