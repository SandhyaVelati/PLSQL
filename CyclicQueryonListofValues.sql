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


CREATE OR REPLACE PROCEDURE get_hier_data(ppi_list IN VARCHAR2,result_cursor OUT SYS_REFCURSOR)  AS 
    v_target_id NUMBER; 
    v_up_id NUMBER; 
    v_lvl NUMBER; 
    v_up_lvl_data T_HIERARCHY_NODE_ROW; 
    v_key_attribute Number;
    v_up_node_count NUMBER; 

	-- Temporary table for holding results
    -- TYPE t_hierarchy_node_table IS TABLE OF T_HIERARCHY_NODE_ROW INDEX BY PLS_INTEGER;
    v_hierarchy_nodes t_hierarchy_node_table := t_hierarchy_node_table();
    v_index PLS_INTEGER := 0;

    CURSOR distinct_key_cursor IS
        SELECT DISTINCT v_up_lvl_data.up_id
        FROM hierarchyTable
        WHERE type = 4;

BEGIN 
    FOR i IN ( 
        SELECT TRIM(REGEXP_SUBSTR(ppi_list, '[^,]+', 1, LEVEL)) l 
        FROM dual 
        CONNECT BY LEVEL <= REGEXP_COUNT(ppi_list, ',') + 1 
    ) LOOP 
        v_up_lvl_data := T_HIERARCHY_NODE_ROW(NULL, NULL, NULL, NULL, NULL); 
        BEGIN 
            SELECT i.l AS TARGET_ppi, child AS up_id, lvl, 0 AS up_node_count 
            INTO v_up_lvl_data.TARGET_ppi, v_up_lvl_data.up_id, v_up_lvl_data.lvl, v_up_lvl_data.up_node_count 
            FROM ( 
                SELECT child, parent, LEVEL AS lvl 
                FROM hierarchyTable 
                WHERE type = 4 
                START WITH child = (SELECT CLT_ID FROM ppi WHERE ppi_ID = i.l) 
                CONNECT BY NOCYCLE PRIOR parent = child AND type = 4 
            ) 
            WHERE parent IS NULL; 
        EXCEPTION 
            WHEN NO_DATA_FOUND THEN 
                DBMS_OUTPUT.PUT_LINE('No data found for ' || i.l); 
        END; 

        -- DBMS_OUTPUT.PUT_LINE(v_up_lvl_data.target_id || ' ' || v_up_lvl_data.up_id || ' ' || v_up_lvl_data.lvl || ' ' || v_up_lvl_data.up_node_count); 

        FOR rec IN distinct_key_cursor LOOP
            v_up_id := rec.up_id;
            
		    SELECT COUNT(*)
            INTO v_up_node_count
    		FROM hierarchyTable
    		CONNECT BY PRIOR CHILD = PARENT
    		START WITH PARENT = v_up_id AND type = 4; 
			IF v_up_lvl_data.up_id = v_up_id THEN
        		v_up_lvl_data.up_node_count := v_up_node_count;
    		END IF;
			SELECT ppi_ID 
            INTO v_up_lvl_data.UP_ppi 
            FROM (SELECT ppi_ID FROM ppi WHERE CLT_ID = v_up_id);
			

			-- Add results to temporary table
            v_hierarchy_nodes.EXTEND;
            v_hierarchy_nodes(v_hierarchy_nodes.COUNT) := v_up_lvl_data;
            -- Output the result
         --    DBMS_OUTPUT.PUT_LINE('Parent: ' || v_up_id || ', Child Count: ' || v_up_node_count);
        	-- DBMS_OUTPUT.PUT_LINE(v_up_lvl_data.target_id || ' ' || v_up_lvl_data.up_id || ' ' || v_up_lvl_data.lvl || ' ' || v_up_lvl_data.up_node_count); 
        END LOOP;
    END LOOP; 
    	-- Open the ref cursor for returning the results
        OPEN result_cursor FOR
        SELECT * FROM TABLE(v_hierarchy_nodes);
END;




select * from ppi;

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
    get_hier_data('asd,ghf,eyt', result_cursor);
    
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



