# deconstruct_distribute

## Location
[src/backend/optimizer/plan/initsplan.c:1120-1271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L1120-L1271)

## Overview
Processes qualification clauses from join tree nodes and distributes them to appropriate restriction and join lists during the second phase of join tree deconstruction.

## Definition

```c
struct_distribute(PlannerInfo *root, JoinTreeItem *jtitem)
{
	Node	   *jtnode = jtitem->jtnode;

	if (IsA(jtnode, RangeTblRef))
	{
		int			varno = ((RangeTblRef *) jtnode)->rtindex;

		/* Deal with any securityQuals attached to the RTE */
		if (root->qual_security_level > 0)
			process_security_barrier_quals(root,
										   varno,
										   jtitem);
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;

		/*
		 * Process any lateral-referencing quals that were postponed to this
		 * level by children.
		 */
		distribute_quals_to_rels(root, jtitem->lateral_clauses,
								 jtitem,
								 NULL,
								 root->qual_security_level,
								 jtitem->qualscope,
								 NULL, NULL, NULL,
								 true, false, false,
								 NULL);

		/*
		 * Now process the top-level quals.
		 */
		distribute_quals_to_rels(root, (List *) f->quals,
								 jtitem,
								 NULL,
								 root->qual_security_level,
								 jtitem->qualscope,
								 NULL, NULL, NULL,
								 true, false, false,
								 NULL);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;
		Relids		ojscope;
		List	   *my_quals;
		SpecialJoinInfo *sjinfo;
		List	  **postponed_oj_qual_list;

		/*
		 * Include lateral-referencing quals postponed from children in
		 * my_quals, so that they'll be handled properly in
		 * make_outerjoininfo.  (This is destructive to
		 * jtitem->lateral_clauses, but we won't use that again.)
		 */
		my_quals = list_concat(jtitem->lateral_clauses,
							   (List *) j->quals);

		/*
		 * For an OJ, form the SpecialJoinInfo now, so that we can pass it to
		 * distribute_qual_to_rels.  We must compute its ojscope too.
		 *
		 * Semijoins are a bit of a hybrid: we build a SpecialJoinInfo, but we
		 * want ojscope = NULL for distribute_qual_to_rels.
		 */
		if (j->jointype != JOIN_INNER)
		{
			sjinfo = make_outerjoininfo(root,
										jtitem->left_rels,
										jtitem->right_rels,
										jtitem->inner_join_rels,
										j->jointype,
										j->rtindex,
										my_quals);
			jtitem->sjinfo = sjinfo;
			if (j->jointype == JOIN_SEMI)
				ojscope = NULL;
			else
				ojscope = bms_union(sjinfo->min_lefthand,
									sjinfo->min_righthand);
		}
		else
		{
			sjinfo = NULL;
			ojscope = NULL;
		}

		/*
		 * If it's a left join with a join clause that is strict for the LHS,
		 * then we need to postpone handling of any non-degenerate join
		 * clauses, in case the join is able to commute with another left join
		 * per identity 3.  (Degenerate clauses need not be postponed, since
		 * they will drop down below this join anyway.)
		 */
		if (j->jointype == JOIN_LEFT && sjinfo->lhs_strict)
		{
			postponed_oj_qual_list = &jtitem->oj_joinclauses;

			/*
			 * Add back any commutable lower OJ relids that were removed from
			 * min_lefthand or min_righthand, else the ojscope cross-check in
			 * distribute_qual_to_rels will complain.  Since we are postponing
			 * processing of non-degenerate clauses, this addition doesn't
			 * affect anything except that cross-check.  Real clause
			 * positioning decisions will be made later, when we revisit the
			 * postponed clauses.
			 */
			ojscope = bms_add_members(ojscope, sjinfo->commute_below_l);
			ojscope = bms_add_members(ojscope, sjinfo->commute_below_r);
		}
		else
			postponed_oj_qual_list = NULL;

		/* Process the JOIN's qual clauses */
		distribute_quals_to_rels(root, my_quals,
								 jtitem,
								 sjinfo,
								 root->qual_security_level,
								 jtitem->qualscope,
								 ojscope, jtitem->nonnullable_rels,
								 NULL,	/* incompatible_relids */
								 true,	/* allow_equivalence */
								 false, false,	/* not clones */
								 postponed_oj_qual_list);

		/* And add the SpecialJoinInfo to join_info_list */
		if (sjinfo)
			root->join_info_list = lappend(root->join_info_list, sjinfo);
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
	}
}

/*
 * process_security_barrier_quals
 *	  Transfer security-barrier quals into relation's baserestrictinfo list.
 *
 * The rewriter put any relevant security-barrier conditions into the RTE's
 * securityQuals field, but it's now time to copy them into the rel's
 * baserestrictinfo.
 *
 * In inheritance cases, we only consider quals attached to the parent rel
 * here;
```
## Detailed Description
This function represents phase 2 of the join tree deconstruction process, responsible for taking qualification clauses extracted during the recursive scan and distributing them to their appropriate locations in the query plan structure. It handles different join tree node types with specialized processing:

**RangeTblRef nodes**: Processes any security barrier qualifications attached to the range table entry, ensuring proper handling of row-level security constraints.

**FromExpr nodes**: Handles both lateral-referencing qualifications that were postponed from child nodes and top-level WHERE clause qualifications, distributing them appropriately using distribute_quals_to_rels.

**JoinExpr nodes**: Performs the most complex processing:
- Creates SpecialJoinInfo structures for outer joins to track join semantics
- Handles postponed lateral clauses by incorporating them into join qualifications
- Implements special logic for LEFT JOINs with LHS-strict clauses, postponing non-degenerate clauses to enable join commutativity optimizations
- Manages ojscope calculation for proper qualification placement
- Adds SpecialJoinInfo entries to root->join_info_list for later use in join planning

The function ensures that qualification clauses are distributed to the correct RelOptInfo nodes while respecting outer join semantics and lateral reference constraints.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning state and target lists
- : The JoinTreeItem containing node information and collected qualification clauses

## Dependencies
- Functions called/Symbols referenced:
  - [process_security_barrier_quals](../p/process_security_barrier_quals.md)
  - [distribute_quals_to_rels](distribute_quals_to_rels.md)
  - [list_concat](../l/list_concat.md)
  - [make_outerjoininfo](../m/make_outerjoininfo.md)
  - [bms_union](../b/bms_union.md)
  - [bms_add_members](../b/bms_add_members.md)
  - nodeTag
- Called from (representative examples):
  - [deconstruct_jointree](deconstruct_jointree.md)

## Notes and Other Information
- Operates in the second phase after deconstruct_recurse has built the join tree structure
- Handles security barrier processing for row-level security enforcement
- Implements sophisticated postponement logic for LEFT JOIN clauses to enable join reordering optimizations
- The postponed_oj_qual_list mechanism allows handling of commutable left joins per algebraic identity 3
- Uses ojscope to control where outer join clauses can be placed relative to the join structure
- Creates SpecialJoinInfo nodes that guide later join planning decisions about valid join orders
- Distinction between degenerate and non-degenerate clauses affects postponement decisions
- Critical for ensuring SQL outer join semantics are preserved during optimization