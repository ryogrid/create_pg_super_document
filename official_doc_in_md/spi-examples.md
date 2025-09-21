45.6. Examples  
---  
[Prev](spi-visibility.md "45.5. Visibility of Data Changes") | [Up](spi.md "Chapter 45. Server Programming Interface")| Chapter 45. Server Programming Interface| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](bgworker.md "Chapter 46. Background Worker Processes")  
  
* * *

## 45.6. Examples #

This section contains a very simple example of SPI usage. The C function `execq` takes an SQL command as its first argument and a row count as its second, executes the command using `SPI_exec` and returns the number of rows that were processed by the command. You can find more complex examples for SPI in the source tree in `src/test/regress/regress.c` and in the [spi](contrib-spi.md "F.39. spi — Server Programming Interface features/examples") module. 
    
    
    #include "postgres.h"
    
    #include "executor/spi.h"
    #include "utils/builtins.h"
    
    PG_MODULE_MAGIC;
    
    PG_FUNCTION_INFO_V1(execq);
    
    Datum
    execq(PG_FUNCTION_ARGS)
    {
        char *command;
        int cnt;
        int ret;
        uint64 proc;
    
        /* Convert given text object to a C string */
        command = text_to_cstring(PG_GETARG_TEXT_PP(0));
        cnt = PG_GETARG_INT32(1);
    
        SPI_connect();
    
        ret = SPI_exec(command, cnt);
    
        proc = SPI_processed;
    
        /*
         * If some rows were fetched, print them via elog(INFO).
         */
        if (ret > 0 && SPI_tuptable != NULL)
        {
            SPITupleTable *tuptable = SPI_tuptable;
            TupleDesc tupdesc = tuptable->tupdesc;
            char buf[8192];
            uint64 j;
    
            for (j = 0; j < tuptable->numvals; j++)
            {
                HeapTuple tuple = tuptable->vals[j];
                int i;
    
                for (i = 1, buf[0] = 0; i <= tupdesc->natts; i++)
                    snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), " %s%s",
                            SPI_getvalue(tuple, tupdesc, i),
                            (i == tupdesc->natts) ? " " : " |");
                elog(INFO, "EXECQ: %s", buf);
            }
        }
    
        SPI_finish();
        pfree(command);
    
        PG_RETURN_INT64(proc);
    }
    

This is how you declare the function after having compiled it into a shared library (details are in [Section 36.10.5](xfunc-c.md#DFUNC "36.10.5. Compiling and Linking Dynamically-Loaded Functions").): 
    
    
    CREATE FUNCTION execq(text, integer) RETURNS int8
        AS '_filename_ '
        LANGUAGE C STRICT;
    

Here is a sample session: 
    
    
    => SELECT execq('CREATE TABLE a (x integer)', 0);
     execq
    -------
         0
    (1 row)
    
    => INSERT INTO a VALUES (execq('INSERT INTO a VALUES (0)', 0));
    INSERT 0 1
    => SELECT execq('SELECT * FROM a', 0);
    INFO:  EXECQ:  0    _-- inserted by execq_
    INFO:  EXECQ:  1    _-- returned by execq and inserted by upper INSERT_
    
     execq
    -------
         2
    (1 row)
    
    => SELECT execq('INSERT INTO a SELECT x + 2 FROM a RETURNING *', 1);
    INFO:  EXECQ:  2    _-- 0 + 2, then execution was stopped by count_
     execq
    -------
         1
    (1 row)
    
    => SELECT execq('SELECT * FROM a', 10);
    INFO:  EXECQ:  0
    INFO:  EXECQ:  1
    INFO:  EXECQ:  2
    
     execq
    -------
         3              _-- 10 is the max value only, 3 is the real number of rows_
    (1 row)
    
    => SELECT execq('INSERT INTO a SELECT x + 10 FROM a', 1);
     execq
    -------
         3              _-- all rows processed; count does not stop it, because nothing is returned_
    (1 row)
    
    => SELECT * FROM a;
     x
    ----
      0
      1
      2
     10
     11
     12
    (6 rows)
    
    => DELETE FROM a;
    DELETE 6
    => INSERT INTO a VALUES (execq('SELECT * FROM a', 0) + 1);
    INSERT 0 1
    => SELECT * FROM a;
     x
    ---
     1                  _-- 0 (no rows in a) + 1_
    (1 row)
    
    => INSERT INTO a VALUES (execq('SELECT * FROM a', 0) + 1);
    INFO:  EXECQ:  1
    INSERT 0 1
    => SELECT * FROM a;
     x
    ---
     1
     2                  _-- 1 (there was one row in a) + 1_
    (2 rows)
    
    _-- This demonstrates the data changes visibility rule._
    _-- execq is called twice and sees different numbers of rows each time:_
    
    => INSERT INTO a SELECT execq('SELECT * FROM a', 0) * x FROM a;
    INFO:  EXECQ:  1    _-- results from first execq_
    INFO:  EXECQ:  2
    INFO:  EXECQ:  1    _-- results from second execq_
    INFO:  EXECQ:  2
    INFO:  EXECQ:  2
    INSERT 0 2
    => SELECT * FROM a;
     x
    ---
     1
     2
     2                  _-- 2 rows * 1 (x in first row)_
     6                  _-- 3 rows (2 + 1 just inserted) * 2 (x in second row)_
    (4 rows)
    

* * *

[Prev](spi-visibility.md "45.5. Visibility of Data Changes") | [Up](spi.md "Chapter 45. Server Programming Interface")|  [Next](bgworker.md "Chapter 46. Background Worker Processes")  
---|---|---  
45.5. Visibility of Data Changes | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 46. Background Worker Processes
