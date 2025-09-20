# ecpg_build_compat_sqlda

## Location
[src/interfaces/ecpg/ecpglib/sqlda.c:205-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/sqlda.c#L205-L254)

## Overview
Builds a compatibility SQLDA (SQL Descriptor Area) structure from a PostgreSQL result set, allocating metadata for all fields while leaving space for field values in a specified row.

## Definition

```c
enum COMPAT_MODE compat)
{
	struct sqlda_compat *sqlda;
	struct sqlvar_compat *sqlvar;
	char	   *fname;
	long		size;
	int			sqld;
	int			i;

	size = sqlda_compat_total_size(res, row, compat);
	sqlda = (struct sqlda_compat *) ecpg_alloc(size, line);
	if (!sqlda)
		return NULL;

	memset(sqlda, 0, size);
	sqlvar = (struct sqlvar_compat *) (sqlda + 1);
	sqld = PQnfields(res);
	fname = (char *) (sqlvar + sqld);

	sqlda->sqld = sqld;
	ecpg_log("ecpg_build_compat_sqlda on line %d sqld = %d\n", line, sqld);
	sqlda->desc_occ = size;		/* cheat here, keep the full allocated size */
	sqlda->sqlvar = sqlvar;

	for (i = 0; i < sqlda->sqld; i++)
	{
		sqlda->sqlvar[i].sqltype = sqlda_dynamic_type(PQftype(res, i), compat);
		strcpy(fname, PQfname(res, i));
		sqlda->sqlvar[i].sqlname = fname;
		fname += strlen(sqlda->sqlvar[i].sqlname) + 1;

		/*
		 * this is reserved for future use, so we leave it empty for the time
		 * being
		 */
		/* sqlda->sqlvar[i].sqlformat = (char *) (long) PQfformat(res, i); */
		sqlda->sqlvar[i].sqlxid = PQftype(res, i);
		sqlda->sqlvar[i].sqltypelen = PQfsize(res, i);
	}

	return sqlda;
}

/*
 * Sets values from PGresult.
 */
static int16 value_is_null = -1;
```
## Detailed Description
This function constructs a  structure that contains metadata about the columns in a PostgreSQL query result. The SQLDA (SQL Descriptor Area) is a data structure used in embedded SQL programming to describe the format and characteristics of dynamic SQL statements. This function specifically builds the compatibility version of SQLDA, which maintains backward compatibility with older ECPG (Embedded C for PostgreSQL) applications.

The function allocates a single memory block that contains the main SQLDA structure, an array of SQLVAR structures (one per column), and space for column names. It populates the metadata fields including SQL types, column names, type identifiers, and type lengths for each field in the result set.

## Parameters
- `line`: Line number in the source code where this function is called (used for debugging and error reporting)
- `res`: PostgreSQL result set (PGresult*) containing the query results and metadata
- `row`: Row number for which space should be allocated (though this parameter appears to be used primarily for size calculation)
- `compat`: Compatibility mode enumeration that determines how SQL types are mapped

## Dependencies
- Functions called/Symbols referenced:
  - [sqlda_compat_total_size](../s/sqlda_compat_total_size.md)
  - ecpg_alloc
  - [PQnfields](../P/PQnfields.md)
  - [ecpg_log](ecpg_log.md)
  - [sqlda_dynamic_type](../s/sqlda_dynamic_type.md)
  - PQftype
  - [PQfname](../P/PQfname.md)
  - PQfsize
- Called from (representative examples):
  - [ECPGdescribe](../E/ECPGdescribe.md)
  - ecpg_process_output

## Notes and Other Information
- The function allocates memory using  which includes line number tracking for debugging
- The  field is set to the total allocated size as a "cheat" to keep track of the full allocation
- The  field is currently reserved for future use and left empty
- Memory layout is carefully designed with the main structure, followed by an array of sqlvar structures, followed by column name strings
- The function returns NULL if memory allocation fails
- All allocated memory is zero-initialized before population