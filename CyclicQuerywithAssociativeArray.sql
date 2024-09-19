
CREATE TABLE  hierarchyTable(  
    id NUMBER,  
    child NUMBER,  
    parent NUMBER,  
    type NUMBER DEFAULT 4  
) ;

CREATE TABLE  ppi(  
    id NUMBER,  
    clt_id NUMBER,  
    ppi_id varchar(10)  
) ;



drop type t_hierarchy_node_table;
create or replace TYPE T_HIERARCHY_NODE_ROW
AS
OBJECT
(
    target_ppi VARCHAR(100 CHAR),
    up_id NUMBER,
    up_ppi VARCHAR(100 CHAR),
    lvl NUMBER,
    up_node_count Number
);

-- Temporary table for holding results
CREATE OR REPLACE TYPE t_hierarchy_node_table AS TABLE OF T_HIERARCHY_NODE_ROW;

-- -- temp table to save rows
-- CREATE GLOBAL TEMPORARY TABLE temp_hierarchy_node_row (
--     target_ppi VARCHAR2(100 CHAR),
--     up_id NUMBER,
--     up_ppi VARCHAR2(100 CHAR),
--     lvl NUMBER,
--     up_node_count NUMBER
-- ) ON COMMIT DELETE ROWS;

-- drop table temp_hierarchy_node_row;

CREATE OR REPLACE PROCEDURE get_hier_data(
    epi_list IN VARCHAR2,
    result_cursor OUT SYS_REFCURSOR
) AS 
    v_up_id NUMBER; 
    v_up_lvl_data T_HIERARCHY_NODE_ROW; 
    v_up_node_count NUMBER; 

	-- Nested table for holding results
    v_hierarchy_nodes t_hierarchy_node_table := t_hierarchy_node_table();

    -- Declare an associative array to hold unique up_id values
    TYPE t_distinct_up_id IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    v_distinct_up_ids t_distinct_up_id;

    v_index PLS_INTEGER := 0;
    v_key_count PLS_INTEGER := 0;

BEGIN 
    -- Initialize v_up_lvl_data
    v_up_lvl_data := T_HIERARCHY_NODE_ROW(NULL, Null,NULL, NULL, NULL); 

    -- Step 1: Process each item in the comma-separated list and build the hierarchy nodes
    FOR i IN ( 
        SELECT TRIM(REGEXP_SUBSTR(epi_list, '[^,]+', 1, LEVEL)) AS l 
        FROM dual 
        CONNECT BY LEVEL <= REGEXP_COUNT(epi_list, ',') + 1 
    ) LOOP 
        BEGIN 
            -- Try to get the hierarchical data for the current PPI
            SELECT i.l AS TARGET_ppi, child AS up_id, lvl, 0 AS up_node_count, NULL AS up_ppi
            INTO v_up_lvl_data.TARGET_ppi, v_up_lvl_data.up_id, v_up_lvl_data.lvl, v_up_lvl_data.up_node_count, v_up_lvl_data.up_ppi
            FROM ( 
                SELECT child, parent, LEVEL AS lvl 
                FROM hierarchyTable 
                WHERE type = 4 
                START WITH child = (SELECT CLT_ID FROM ppi WHERE ppi_ID = i.l) 
                CONNECT BY NOCYCLE PRIOR parent = child AND type = 4 
            ) 
            WHERE parent IS NULL;

            -- Add the up_id to the distinct set if it's not null
            IF v_up_lvl_data.up_id IS NOT NULL THEN
                v_key_count := v_key_count + 1;
                v_distinct_up_ids(v_key_count) := v_up_lvl_data.up_id;
            END IF;

        EXCEPTION 
            WHEN NO_DATA_FOUND THEN 
                -- If no hierarchy is found, add a row with null values for hierarchy data
                v_up_lvl_data := T_HIERARCHY_NODE_ROW(i.l, NULL, NULL, NULL, 0);
        END;

        -- Add the current row to the nested table collection
        v_hierarchy_nodes.EXTEND;
        v_hierarchy_nodes(v_hierarchy_nodes.COUNT) := v_up_lvl_data;

    END LOOP;

    -- Step 2: For each distinct up_id, calculate up_node_count and up_ppi
    FOR i IN 1..v_key_count LOOP
        v_up_id := v_distinct_up_ids(i);

        -- Calculate the node count for the current up_id
        SELECT COUNT(*)
        INTO v_up_node_count
        FROM hierarchyTable
        CONNECT BY PRIOR CHILD = PARENT
        START WITH PARENT = v_up_id AND type = 4;  

        -- Get the up_ppi for the current up_id
        SELECT ppi_ID 
        INTO v_up_lvl_data.UP_ppi 
        FROM ppi 
        WHERE CLT_ID = v_up_id;

        -- Update the nested table with the calculated up_node_count and up_ppi
        FOR j IN 1..v_hierarchy_nodes.COUNT LOOP
            IF v_hierarchy_nodes(j).up_id = v_up_id THEN
                v_hierarchy_nodes(j).up_node_count := v_up_node_count;
                v_hierarchy_nodes(j).up_ppi := v_up_lvl_data.UP_ppi;
            END IF;
        END LOOP;
    END LOOP;

    -- Step 3: Return the results using the cursor
    OPEN result_cursor FOR
    SELECT * FROM TABLE(v_hierarchy_nodes);
END;

-- =============================calling block===============================
DECLARE 
    -- v_cur SYS_REFCURSOR; 
    -- Define the REF CURSOR type to handle the results
    TYPE ref_cursor IS REF CURSOR;

    -- Variables for holding the REF CURSOR and fetched data
    result_cursor ref_cursor;
    v_target_ppi VARCHAR(100 CHAR);
    v_up_ppi VARCHAR(100 CHAR);
    v_up_id NUMBER;
    v_lvl NUMBER;
    v_up_node_count NUMBER; 
BEGIN 
    -- Call the get_hier_data procedure
    get_hier_data('asd,def,hgd,tre,fdf,zse', result_cursor);
    
    -- Fetch and process the results from the REF CURSOR
    LOOP
        FETCH result_cursor INTO  v_target_ppi, v_up_id,v_up_ppi, v_lvl, v_up_node_count;
        EXIT WHEN result_cursor%NOTFOUND;

        -- Process each row (example: output to console)
        DBMS_OUTPUT.PUT_LINE(' v_target_ppi: ' || v_target_ppi||' v_up_ppi: ' || v_up_ppi|| '	Up CLT OID: ' || v_up_id ||'	Level: ' || v_lvl || '	Up Node Count: ' || v_up_node_count);
    END LOOP;
    
    -- Close the REF CURSOR
    CLOSE result_cursor;
EXCEPTION
    WHEN OTHERS THEN
        -- Handle exceptions and ensure the REF CURSOR is closed
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
        IF result_cursor%ISOPEN THEN
            CLOSE result_cursor;
        END IF;
 
END; 

-- ===================================end of calling block===========================

-- INSERT INTO hierarchyTable (id, child,  type) VALUES (1, 2022, 4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (2, 1022, 2022, 4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (3, 1048, 2022, 4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (4, 1089, 1022, 4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (5, 1534, 1089, 4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (6, 1532, 1089, 4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (7, 1097, 1022, 4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (8, 1433, 1097, 4);

-- INSERT INTO hierarchyTable (id, child, type) VALUES (9, 2029,  4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (10, 1078, 2029, 4);
-- INSERT INTO hierarchyTable (id, child, parent, type) VALUES (11, 1555, 2029, 4);

-- insert into ppi(id, clt_id, ppi_id) values(1,2022,'asd');
-- insert into ppi(id, clt_id, ppi_id) values(2,1022,'def');
-- insert into ppi(id, clt_id, ppi_id) values(3,1048,'mkh');
-- insert into ppi(id, clt_id, ppi_id) values(4,1089,'arn');
-- insert into ppi(id, clt_id, ppi_id) values(5,1534,'pou');
-- insert into ppi(id, clt_id, ppi_id) values(6,1532,'heg');
-- insert into ppi(id, clt_id, ppi_id) values(7,1097,'lui');
-- insert into ppi(id, clt_id, ppi_id) values(8,1433,'fdf');
-- insert into ppi(id, clt_id, ppi_id) values(9,2029,'eyu');
-- insert into ppi(id, clt_id, ppi_id) values(10,1078,'tre');
-- insert into ppi(id, clt_id, ppi_id) values(11,1555,'zse');

--           SELECT 'lui' AS TARGET_ppi, child AS up_id, lvl, 0 AS up_node_count ,null as up_ppi
--             -- INTO v_up_lvl_data.TARGET_ppi, v_up_lvl_data.up_id, v_up_lvl_data.lvl, v_up_lvl_data.up_node_count ,v_up_lvl_data.up_ppi
--             FROM ( 
--                 SELECT child, parent, LEVEL AS lvl 
--                 FROM hierarchyTable 
--                 WHERE type = 4 
--                 START WITH child =(SELECT CLT_ID FROM ppi WHERE ppi_ID = 'lui') 
--                 CONNECT BY NOCYCLE PRIOR parent = child AND type = 4 
--             ) 
--             WHERE parent IS NULL; 

--   SELECT COUNT(*)
--             -- INTO v_up_node_count
--     		FROM hierarchyTable
--     		CONNECT BY PRIOR CHILD = PARENT
--     		START WITH PARENT = 2022 AND type = 4;  
select * from hierarchyTable;



CREATE OR REPLACE PROCEDURE get_hier_data(epi_list IN VARCHAR2, result_cursor OUT SYS_REFCURSOR) AS
    v_up_id NUMBER;
    v_up_node_count NUMBER;
    v_up_ppi VARCHAR2(100 CHAR);
    v_up_lvl_data t_hierarchy_node;
    v_hierarchy_nodes t_hierarchy_node_table := t_hierarchy_node_table();
BEGIN
    
    -- Split the epi_list and insert into the temporary table
    FOR i IN (
        SELECT TRIM(REGEXP_SUBSTR(epi_list, '[^,]+', 1, LEVEL)) l
        FROM dual
        CONNECT BY LEVEL <= REGEXP_COUNT(epi_list, ',') + 1
    ) LOOP
        BEGIN
            INSERT INTO temp_hierarchy_node_row (target_ppi, up_id, lvl, up_node_count, up_ppi)
            SELECT i.l AS target_ppi, child AS up_id, LEVEL AS lvl, 0 AS up_node_count, NULL AS up_ppi
            FROM hierarchyTable
            WHERE type = 4
            START WITH child = (SELECT CLT_ID FROM ppi WHERE ppi_ID = i.l)
            CONNECT BY NOCYCLE PRIOR parent = child AND type = 4
            AND parent IS NULL;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO temp_hierarchy_node_row (target_ppi, up_id, lvl, up_node_count, up_ppi)
                VALUES (i.l, NULL, NULL, 0, NULL);
        END;
    END LOOP;

    -- Populate the collection from the temporary table
    FOR rec IN (SELECT * FROM temp_hierarchy_node_row) LOOP
        v_up_lvl_data := t_hierarchy_node(rec.target_ppi, rec.up_id, rec.up_ppi, rec.lvl, rec.up_node_count);
        v_hierarchy_nodes.EXTEND;
        v_hierarchy_nodes(v_hierarchy_nodes.COUNT) := v_up_lvl_data;
    END LOOP;

    -- Update the collection with node counts and ppi values
    FOR rec IN (SELECT DISTINCT up_id FROM temp_hierarchy_node_row) LOOP
        v_up_id := rec.up_id;

        -- Calculate the node count
        SELECT COUNT(*)
        INTO v_up_node_count
        FROM hierarchyTable
        CONNECT BY PRIOR CHILD = PARENT
        START WITH PARENT = v_up_id AND type = 4;

        -- Get the UP_ppi value
        SELECT ppi_ID
        INTO v_up_ppi
        FROM ppi
        WHERE CLT_ID = v_up_id;

        -- Update the collection
        FOR i IN 1..v_hierarchy_nodes.COUNT LOOP
            IF v_hierarchy_nodes(i).up_id = v_up_id THEN
                v_hierarchy_nodes(i).up_node_count := v_up_node_count;
                v_hierarchy_nodes(i).up_ppi := v_up_ppi;
            END IF;
        END LOOP;
    END LOOP;

    -- Open the ref cursor for returning the results
    OPEN result_cursor FOR
    SELECT * FROM TABLE(v_hierarchy_nodes);
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;














