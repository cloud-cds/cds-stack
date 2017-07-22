--
-- A sql extenstion that parallizes sql queries using dblink
-- Author: Copyright (c) 2015 Klaus Ackermann  <klaus.ackermann@monash.edu>
-- Written during the The Eric & Wendy Schmidt Data Science for Social Good Fellowship at University of Chicago 2015.
--

-- init: read README.md to learn how to setup dblink
-- test case 1: two procs operate on different tables
-- select * from distribute('dblink_dist', array['drop table if exists pat_enc_t0; create unlogged table pat_enc_t0 as select * from pat_enc where dataset_id = 1 limit 10;', 'drop table if exists pat_enc_t1; create unlogged table pat_enc_t1 as select * from pat_enc where dataset_id = 3 limit 10;'],2);

-- test case 2: two procs operate on the same table
-- create a test table: create unlogged table if not exists pat_enc_test as select * from pat_enc limit 10000;
-- select * from distribute('dblink_dist', array['update pat_enc_test set dept_id = dataset_id where dataset_id < 3;', 'update pat_enc_test set dept_id = -dataset_id where dataset_id > 3;'],2);

-- test case 3: seven procs operate on the same table
-- create a test table: create unlogged table if not exists pat_enc_test as select * from pat_enc;
-- select * from distribute('dblink_dist', array['update pat_enc_test set dept_id = ''d1'' where dataset_id = 1;', 'update pat_enc_test set dept_id = ''d2'' where dataset_id = 2;', 'update pat_enc_test set dept_id = ''d3'' where dataset_id = 3;', 'update pat_enc_test set dept_id = ''d4'' where dataset_id = 4;', 'update pat_enc_test set dept_id = ''d5'' where dataset_id = 5;', 'update pat_enc_test set dept_id = ''d6'' where dataset_id = 6;', 'update pat_enc_test set dept_id = ''d7'' where dataset_id = 7;'],7);

CREATE OR REPLACE FUNCTION distribute(db text, query_array text[], num_procs integer default 2)
  RETURNS text language plpgsql
  stable
  AS
$function$
DECLARE
  sql     TEXT;
  num_chunks bigint;
  query text;
  i bigint;
  conn text;
  n bigint;
  num_done bigint;
  status bigint;
  dispatch_result bigint;
  dispatch_error text;
  array_procs int[];
  current_proc int;
  used_procs int;
  now_t timestamptz;
  done int;

BEGIN
  num_chunks := array_length(query_array, 1);
  RAISE NOTICE 'Total number of chunks:  %',num_chunks;

  --initialize array for keeping track of finished processes
  sql := 'SELECT array_fill(0, ARRAY[' || num_procs ||']);';
  EXECUTE sql into array_procs;

  current_proc := 0;
  used_procs := 0;
  -- loop through chunks
  i := 0;
  <<chunk_loop>>
  FOREACH query in ARRAY query_array
  LOOP
    i := i + 1;
    RAISE NOTICE 'Query %: %', i ,query;

    current_proc := current_proc + 1;
    array_procs[current_proc] = 0;
    used_procs := used_procs + 1;


    --make a new db connection
    conn := 'conn_' || current_proc;
    RAISE NOTICE 'New Connection name: %',conn;

    sql := 'SELECT dblink_connect(' || QUOTE_LITERAL(conn) || ',' || QUOTE_LITERAL(db) ||');';
    raise NOTICE '%', sql;
    execute sql;


    --send the query asynchronously using the dblink connection
    sql := 'SELECT dblink_send_query(' || QUOTE_LITERAL(conn) || ',' || QUOTE_LITERAL(query) || ');';
    raise NOTICE '%', sql;
    execute sql into dispatch_result;

    -- check for errors dispatching the query
    if dispatch_result = 0 then
       sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
       execute sql into dispatch_error;
       RAISE 'Error: %', dispatch_error;
    end if;

    --check how many processors are in use right now
    if (i<>(num_chunks+1)) and  used_procs>=num_procs then
          done := 0 ;
  --repetatly check until one proc is finished to relaunch the next chunck
    Loop
      for n in 1..num_procs
      Loop
        conn := 'conn_' || n;
        sql := 'SELECT dblink_is_busy(' || QUOTE_LITERAL(conn) || ');';
        execute sql into status;
        if status = 0 THEN
          -- check for error messages
          sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
          execute sql into dispatch_error;
          if dispatch_error <> 'OK' THEN
            RAISE '%', dispatch_error;
          end if;

          --terminate the connection and resect the active proc counter so that the next
          --connection is started with the correct index
          RAISE NOTICE 'Process done:  % at %, Next Chunk to be started: %',conn, timeofday(), i+1;

          --disconnect the connection
          sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
          execute sql;

          current_proc := n - 1; --as the counter gets increased at the beginning of the other loop
          used_procs := used_procs - 1;
          done := 1;
          array_procs[n]=1;

          exit; --terminate the loop
        END if;
      end loop;
      if done = 1 then
        exit;
      end if;
      sql := 'select pg_sleep(0.5)';
      execute sql;
    END loop;
  END IF;
end loop chunk_loop;
-- wait until all queries are finished
  Loop
    for i in 1..num_procs
    Loop
    if array_procs[i]<>1 THEN
      conn := 'conn_' || i;
      sql := 'SELECT dblink_is_busy(' || QUOTE_LITERAL(conn) || ');';
      execute sql into status;
      if status = 0 THEN
        -- check for error messages
        sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
        execute sql into dispatch_error;
        if dispatch_error <> 'OK' THEN
          RAISE '%', dispatch_error;
        end if;
        used_procs := used_procs - 1;

        --disconnect the connection
        sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
        execute sql;
        RAISE NOTICE 'Process done:  % at %',conn, timeofday();
        array_procs[i]=1;
      END if;
    END if;
    end loop;
    --if num_done >= num_procs then
    if used_procs <= 0 then
    exit;
    end if;
    --pause in poling
    sql := 'select pg_sleep(1)';
    execute sql;

  END loop;


  RETURN 'Success';

-- error catching to disconnect dblink connections, if error occurs
exception when others then
  BEGIN
  RAISE NOTICE '% %', SQLERRM, SQLSTATE;
  for n in
  SELECT generate_series(1,i) as n
  LOOP
    conn := 'conn_' || n;

    -- cancel a previous crashed query
    sql := 'SELECT dblink_cancel_query(' || QUOTE_LITERAL(conn) ||');';
    execute sql;

    sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
    execute sql;
  END LOOP;
  exception when others then
    RAISE NOTICE '% %', SQLERRM, SQLSTATE;
  end;
END
$function$;
