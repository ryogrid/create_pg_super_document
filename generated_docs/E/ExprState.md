# ExprState

## Location
[src/include/nodes/execnodes.h:78-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L78-L141)

## Overview
ExprState is the core runtime state structure for expression evaluation in PostgreSQL's executor, containing compiled instructions and storage for expression results.

## Definition

```c
typedef struct ExprState
{
	NodeTag		type;

	uint8		flags;			/* bitmask of EEO_FLAG_* bits, see above */

	/*
	 * Storage for result value of a scalar expression, or for individual
	 * column results within expressions built by ExecBuildProjectionInfo().
	 */
#define FIELDNO_EXPRSTATE_RESNULL 2
	bool		resnull;
#define FIELDNO_EXPRSTATE_RESVALUE 3
	Datum		resvalue;

	/*
	 * If projecting a tuple result, this slot holds the result; else NULL.
	 */
#define FIELDNO_EXPRSTATE_RESULTSLOT 4
	TupleTableSlot *resultslot;

	/*
	 * Instructions to compute expression's return value.
	 */
	struct ExprEvalStep *steps;

	/*
	 * Function that actually evaluates the expression.  This can be set to
	 * different values depending on the complexity of the expression.
	 */
	ExprStateEvalFunc evalfunc;

	/* original expression tree, for debugging only */
	Expr	   *expr;

	/* private state for an evalfunc */
	void	   *evalfunc_private;

	/*
	 * XXX: following fields only needed during "compilation" (ExecInitExpr);
	 * could be thrown away afterwards.
	 */

	int			steps_len;		/* number of steps currently */
	int			steps_alloc;	/* allocated length of steps array */

#define FIELDNO_EXPRSTATE_PARENT 11
	struct PlanState *parent;	/* parent PlanState node, if any */
	ParamListInfo ext_params;	/* for compiling PARAM_EXTERN nodes */

	Datum	   *innermost_caseval;
	bool	   *innermost_casenull;

	Datum	   *innermost_domainval;
	bool	   *innermost_domainnull;

	/*
	 * For expression nodes that support soft errors. Should be set to NULL if
	 * the caller wants errors to be thrown. Callers that do not want errors
	 * thrown should set it to a valid ErrorSaveContext before calling
	 * ExecInitExprRec().
	 */
	ErrorSaveContext *escontext;
} ExprState;
```
## Detailed Description
ExprState represents the compiled, executable form of an expression in PostgreSQL's executor. It transforms parse tree expressions into a sequence of evaluation steps that can be efficiently executed. The structure supports both scalar expression evaluation and tuple projection operations. The compilation process converts expression trees into a linear sequence of evaluation steps stored in the  array, which is then executed by the function pointer in . This design allows for optimized expression evaluation with minimal overhead during query execution.

## Parameters / Member Variables
- : Standard PostgreSQL node tag for type identification
- : Bitmask containing EEO_FLAG_* bits controlling evaluation behavior
- : Boolean flag indicating if the expression result is NULL
- : Datum storage for scalar expression results
- : TupleTableSlot pointer for tuple projection results, NULL for scalar expressions
- : Array of ExprEvalStep structures containing evaluation instructions
- : Function pointer to the actual evaluation function, optimized based on expression complexity
- : Pointer to original expression tree, retained for debugging purposes
- : Opaque pointer to private state data used by the evaluation function
- : Current number of evaluation steps in the steps array
- : Allocated capacity of the steps array
- : Pointer to parent PlanState node in the execution tree
- : ParamListInfo for resolving PARAM_EXTERN parameter nodes
- : Datum storage for CASE expression evaluation context
- : Null flag array for CASE expression evaluation
- : Datum storage for domain constraint checking
- : Null flag for domain constraint checking
- : Error handling context for soft error support during expression compilation

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalStep (evaluation instruction structure)
  - ParamListInfo (external parameter management)
  - ErrorSaveContext (soft error handling framework)
- Called from (representative examples):
  - ExecInitExpr (expression initialization)
  - ExecEvalExpr (expression evaluation)
  - ExecBuildProjectionInfo (projection setup)

## Notes and Other Information
- The structure is designed for high-performance expression evaluation with minimal runtime overhead
- Fields marked with FIELDNO_* macros are accessed by generated code and JIT compilation
- The steps array and related compilation fields could theoretically be discarded after compilation to save memory
- Supports both traditional interpretation and Just-In-Time (JIT) compilation for performance optimization
- The design allows for different evaluation strategies through the pluggable evalfunc mechanism