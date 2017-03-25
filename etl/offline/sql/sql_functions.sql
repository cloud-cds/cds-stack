/*
 * predefined functions
 */
CREATE OR REPLACE FUNCTION merge_cdm_g(key2 TEXT, new_value TEXT, confidence_flag INT) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_g SET value = new_value, confidence = confidence_flag WHERE fid = key2;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_g(fid,value,confidence) VALUES (key2,new_value, confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_s(key1 INT, key2 TEXT, new_value TEXT, confidence_flag Int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_s SET value = new_value, confidence = confidence_flag WHERE enc_id = key1 AND fid = key2;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_s(enc_id,fid,value,confidence) VALUES (key1,key2,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_m(key1 INT, key2 TEXT, key3 INT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_m SET value = new_value, confidence = confidence_flag WHERE enc_id = key1 AND fid = key2 AND line = key3;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_m(enc_id,fid,line,value,confidence) VALUES (key1,key2,key3,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_t(key1 INT, key2 timestamp, key3 TEXT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_t SET value = new_value, confidence=confidence_flag WHERE enc_id = key1 AND tsp = key2 AND fid = key3;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_t(enc_id,tsp,fid,value,confidence) VALUES (key1,key2,key3,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_twf(update_set_cols TEXT, update_set_values TEXT, update_where TEXT, insert_cols TEXT, insert_values TEXT) RETURNS VOID AS
$$
DECLARE
    tmpint  INTEGER := 0;
BEGIN
    LOOP
        -- first try to update the key
        EXECUTE 'UPDATE cdm_twf SET (' || update_set_cols || ') = (' 
            || update_set_values || ') WHERE ' || update_where ;
        GET DIAGNOSTICS tmpint = ROW_COUNT;
        IF tmpint THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            EXECUTE 'INSERT INTO cdm_twf(' || insert_cols 
                || ') VALUES (' || insert_values || ')';
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_cdm_twf_workspace(update_set_cols TEXT, update_set_values TEXT, update_where TEXT, insert_cols TEXT, insert_values TEXT) RETURNS VOID AS
$$
DECLARE
    tmpint  INTEGER := 0;
BEGIN
    LOOP
        -- first try to update the key
        EXECUTE 'UPDATE cdm_twf_workspace SET (' || update_set_cols || ') = (' 
            || update_set_values || ') WHERE ' || update_where ;
        GET DIAGNOSTICS tmpint = ROW_COUNT;
        IF tmpint THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            EXECUTE 'INSERT INTO cdm_twf_workspace(' || insert_cols 
                || ') VALUES (' || insert_values || ')';
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_cdm_g(key2 TEXT, new_value TEXT, confidence_flag INT) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_g SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE fid = key2;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_g(fid,value,confidence) VALUES (key2,new_value, confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_cdm_s(key1 INT, key2 TEXT, new_value TEXT, confidence_flag Int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_s SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE enc_id = key1 AND fid = key2;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_s(enc_id,fid,value,confidence) VALUES (key1,key2,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_cdm_m(key1 INT, key2 TEXT, key3 INT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE cdm_m SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE enc_id = key1 AND fid = key2 AND line = key3;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_m(enc_id,fid,line,value,confidence) VALUES (key1,key2,key3,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION add_cdm_t(key1 INT, key2 timestamp, key3 TEXT, new_value TEXT, confidence_flag int) RETURNS VOID AS
-- $$
-- BEGIN
--     LOOP
--         -- first try to update the key
--         UPDATE cdm_t SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE enc_id = key1 AND tsp = key2 AND fid = key3;
--         IF found THEN
--             RETURN;
--         END IF;
--         -- not there, so try to insert the key
--         -- if someone else inserts the same key concurrently,
--         -- we could get a unique-key failure
--         BEGIN
--             INSERT INTO cdm_t(enc_id,tsp,fid,value,confidence) VALUES (key1,key2,key3,new_value,confidence_flag);
--             RETURN;
--         EXCEPTION WHEN unique_violation THEN
--             -- do nothing, and loop to try the UPDATE again
--         END;
--     END LOOP;
-- END;
-- $$
-- LANGUAGE plpgsql;

-- add_cdm_t for medication summation 
CREATE OR REPLACE FUNCTION add_cdm_t(key1 INT, key2 timestamp, key3 TEXT, new_value TEXT, confidence_flag int) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        IF new_value ~ '^[0-9\.]+$' THEN
            UPDATE cdm_t SET value = cast(value as numeric) + cast(new_value as numeric), confidence = confidence | confidence_flag WHERE enc_id = key1 AND tsp = key2 AND fid = key3;
        ELSE
        
            UPDATE cdm_t SET value = json_object_set_key(value::json, 'dose', 
                (value::json->>'dose')::numeric 
                    + (new_value::json->>'dose')::numeric)::text
                , confidence = confidence | confidence_flag 
                WHERE enc_id = key1 AND tsp = key2 AND fid = key3;
        END IF;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO cdm_t(enc_id,tsp,fid,value,confidence) VALUES (key1,key2,key3,new_value,confidence_flag);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION based_on_popmean(confidence INT) RETURNS integer AS $$
    BEGIN
        IF confidence is null THEN RETURN 0;
        END IF;
        RETURN confidence & 16 >> 4;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION "json_object_del_key"(
  "json"          json,
  "key_to_del"    TEXT
)
  RETURNS json
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT CASE
  WHEN ("json" -> "key_to_del") IS NULL THEN "json"
  ELSE (SELECT concat('{', string_agg(to_json("key") || ':' || "value", ','), '}')
          FROM (SELECT *
                  FROM json_each("json")
                 WHERE "key" <> "key_to_del"
               ) AS "fields")::json
END
$function$;

CREATE OR REPLACE FUNCTION "json_object_set_key"(
  "json"          json,
  "key_to_set"    TEXT,
  "value_to_set"  anyelement
)
  RETURNS json
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT concat('{', string_agg(to_json("key") || ':' || "value", ','), '}')::json
  FROM (SELECT *
          FROM json_each("json")
         WHERE "key" <> "key_to_set"
         UNION ALL
        SELECT "key_to_set", to_json("value_to_set")) AS "fields"
$function$;

-- Create a function that always returns the first non-NULL item
CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$;
 
-- And then wrap an aggregate around it
CREATE AGGREGATE public.FIRST (
        sfunc    = public.first_agg,
        basetype = anyelement,
        stype    = anyelement
);
 
-- Create a function that always returns the last non-NULL item
CREATE OR REPLACE FUNCTION public.last_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $2;
$$;
 
-- And then wrap an aggregate around it
CREATE AGGREGATE public.LAST (
        sfunc    = public.last_agg,
        basetype = anyelement,
        stype    = anyelement
);