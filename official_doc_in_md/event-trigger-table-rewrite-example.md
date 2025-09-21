38.5. A Table Rewrite Event Trigger Example  
---  
[Prev](event-trigger-example.md "38.4. A Complete Event Trigger Example") | [Up](event-triggers.md "Chapter 38. Event Triggers")| Chapter 38. Event Triggers| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](event-trigger-database-login-example.md "38.6. A Database Login Event Trigger Example")  
  
* * *

## 38.5. A Table Rewrite Event Trigger Example #

Thanks to the `table_rewrite` event, it is possible to implement a table rewriting policy only allowing the rewrite in maintenance windows. 

Here's an example implementing such a policy. 
    
    
    CREATE OR REPLACE FUNCTION no_rewrite()
     RETURNS event_trigger
     LANGUAGE plpgsql AS
    $$
    ---
    --- Implement local Table Rewriting policy:
    ---   public.foo is not allowed rewriting, ever
    ---   other tables are only allowed rewriting between 1am and 6am
    ---   unless they have more than 100 blocks
    ---
    DECLARE
      table_oid oid := pg_event_trigger_table_rewrite_oid();
      current_hour integer := extract('hour' from current_time);
      pages integer;
      max_pages integer := 100;
    BEGIN
      IF pg_event_trigger_table_rewrite_oid() = 'public.foo'::regclass
      THEN
            RAISE EXCEPTION 'you''re not allowed to rewrite the table %',
                            table_oid::regclass;
      END IF;
    
      SELECT INTO pages relpages FROM pg_class WHERE oid = table_oid;
      IF pages > max_pages
      THEN
            RAISE EXCEPTION 'rewrites only allowed for table with less than % pages',
                            max_pages;
      END IF;
    
      IF current_hour NOT BETWEEN 1 AND 6
      THEN
            RAISE EXCEPTION 'rewrites only allowed between 1am and 6am';
      END IF;
    END;
    $$;
    
    CREATE EVENT TRIGGER no_rewrite_allowed
                      ON table_rewrite
       EXECUTE FUNCTION no_rewrite();
    

* * *

[Prev](event-trigger-example.md "38.4. A Complete Event Trigger Example") | [Up](event-triggers.md "Chapter 38. Event Triggers")|  [Next](event-trigger-database-login-example.md "38.6. A Database Login Event Trigger Example")  
---|---|---  
38.4. A Complete Event Trigger Example | [Home](index.md "PostgreSQL 17.5 Documentation")|  38.6. A Database Login Event Trigger Example
