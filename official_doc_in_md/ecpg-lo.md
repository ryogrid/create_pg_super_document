34.12. Large Objects  
---  
[Prev](ecpg-library.md "34.11. Library Functions") | [Up](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")| Chapter 34. ECPG — Embedded SQL in C| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-cpp.md "34.13. C++ Applications")  
  
* * *

## 34.12. Large Objects #

Large objects are not directly supported by ECPG, but ECPG application can manipulate large objects through the libpq large object functions, obtaining the necessary `PGconn` object by calling the `ECPGget_PGconn()` function. (However, use of the `ECPGget_PGconn()` function and touching `PGconn` objects directly should be done very carefully and ideally not mixed with other ECPG database access calls.) 

For more details about the `ECPGget_PGconn()`, see [Section 34.11](ecpg-library.md "34.11. Library Functions"). For information about the large object function interface, see [Chapter 33](largeobjects.md "Chapter 33. Large Objects"). 

Large object functions have to be called in a transaction block, so when autocommit is off, `BEGIN` commands have to be issued explicitly. 

[Example 34.2](ecpg-lo.md#ECPG-LO-EXAMPLE "Example 34.2. ECPG Program Accessing Large Objects") shows an example program that illustrates how to create, write, and read a large object in an ECPG application. 

**Example 34.2. ECPG Program Accessing Large Objects**
    
    
    #include <stdio.h>
    #include <stdlib.h>
    #include <libpq-fe.h>
    #include <libpq/libpq-fs.h>
    
    EXEC SQL WHENEVER SQLERROR STOP;
    
    int
    main(void)
    {
        PGconn     *conn;
        Oid         loid;
        int         fd;
        char        buf[256];
        int         buflen = 256;
        char        buf2[256];
        int         rc;
    
        memset(buf, 1, buflen);
    
        EXEC SQL CONNECT TO testdb AS con1;
        EXEC SQL SELECT pg_catalog.set_config('search_path', '', false); EXEC SQL COMMIT;
    
        conn = ECPGget_PGconn("con1");
        printf("conn = %p\n", conn);
    
        /* create */
        loid = lo_create(conn, 0);
        if (loid &lt; 0)
            printf("lo_create() failed: %s", PQerrorMessage(conn));
    
        printf("loid = %d\n", loid);
    
        /* write test */
        fd = lo_open(conn, loid, INV_READ|INV_WRITE);
        if (fd &lt; 0)
            printf("lo_open() failed: %s", PQerrorMessage(conn));
    
        printf("fd = %d\n", fd);
    
        rc = lo_write(conn, fd, buf, buflen);
        if (rc &lt; 0)
            printf("lo_write() failed\n");
    
        rc = lo_close(conn, fd);
        if (rc &lt; 0)
            printf("lo_close() failed: %s", PQerrorMessage(conn));
    
        /* read test */
        fd = lo_open(conn, loid, INV_READ);
        if (fd &lt; 0)
            printf("lo_open() failed: %s", PQerrorMessage(conn));
    
        printf("fd = %d\n", fd);
    
        rc = lo_read(conn, fd, buf2, buflen);
        if (rc &lt; 0)
            printf("lo_read() failed\n");
    
        rc = lo_close(conn, fd);
        if (rc &lt; 0)
            printf("lo_close() failed: %s", PQerrorMessage(conn));
    
        /* check */
        rc = memcmp(buf, buf2, buflen);
        printf("memcmp() = %d\n", rc);
    
        /* cleanup */
        rc = lo_unlink(conn, loid);
        if (rc &lt; 0)
            printf("lo_unlink() failed: %s", PQerrorMessage(conn));
    
        EXEC SQL COMMIT;
        EXEC SQL DISCONNECT ALL;
        return 0;
    }
    

  


* * *

[Prev](ecpg-library.md "34.11. Library Functions") | [Up](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")|  [Next](ecpg-cpp.md "34.13. C++ Applications")  
---|---|---  
34.11. Library Functions | [Home](index.md "PostgreSQL 17.5 Documentation")|  34.13. C++ Applications
