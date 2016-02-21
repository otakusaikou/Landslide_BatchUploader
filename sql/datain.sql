--Insert values (basin_id, basin) from table result which is not exist in table basin
INSERT INTO public.basin
SELECT DISTINCT basin_id, basin
FROM public.result
WHERE basin_id NOT IN (SELECT basin_id
                       FROM public.basin);

--Insert values (forest_id, forest_dis) from table result which is not exist in table forest_district
INSERT INTO public.forest_district
SELECT DISTINCT forest_id, forest_dis
FROM public.result
WHERE forest_id NOT IN (SELECT forest_id
                        FROM public.forest_district);
                       
--Insert values (reserv_id, reservoir) from table result which is not exist in table reservoir
INSERT INTO public.reservoir
SELECT DISTINCT reserv_id, reservoir
FROM public.result
WHERE reserv_id NOT IN (SELECT reservoir_id
                        FROM public.reservoir);
                        
--Insert values (watersh_id, watershed) from table result which is not exist in table watershed
INSERT INTO public.watershed
SELECT DISTINCT watersh_id, watershed
FROM public.result
WHERE watersh_id NOT IN (SELECT water_id
                         FROM public.watershed);
                        
--Insert values (working_id, working_ci) from table result which is not exist in table working_circle
INSERT INTO public.working_circle
SELECT DISTINCT working_id, working_ci
FROM public.result
WHERE working_id NOT IN (SELECT workingcircle_id
                         FROM public.working_circle);
                         
--Insert values (county_id, town_id, county, township) from table result which is not exist in table admin_area
INSERT INTO public.admin_area
SELECT DISTINCT county_id, town_id, county, township
FROM public.result
WHERE (county_id, town_id) NOT IN (SELECT county_id, town_id
                                   FROM public.admin_area);
                                   
                                   
--Insert value dmcdate from table result which is not exist in table project, the sequence is project_id
INSERT INTO public.project 
SELECT nextval('project_id_seq'::regclass), dmcdate::date
FROM (SELECT DISTINCT dmcdate
      FROM public.result
      WHERE dmcdate NOT IN (SELECT project_date
                            FROM public.project)) AS T;
                            
--Insert values [slide_id, centroid_x, centroid_y, area, geom, project_id, county_id, town_id, working_id, reserv_id, watersh_id, forest_id, basin_id] from table result and project which is not exist in table slide_area
INSERT INTO public.slide_area
SELECT nextval('slide_id_seq'::regclass) slide_id, centroid_x, centroid_y, area, geom, project_id, null, input_date, remarks, map_name, county_id, town_id, working_id, reserv_id, watersh_id, forest_id, basin_id
FROM public.result JOIN public.project ON dmcdate = project_date;

DROP TABLE IF EXISTS result;

--Create template view by joining all tables together 
DO $$
BEGIN
IF NOT EXISTS (SELECT * 
               FROM pg_catalog.pg_views 
               WHERE viewname = 'tmp_query')
THEN
    CREATE VIEW tmp_query AS
    SELECT *
    FROM (SELECT slide_id,  project_date::text, workingcircle_name, forest_name, county_name, town_name, reservoir_name, water_name, basin_name, input_date, map_name, remarks, area, centroid_x, centroid_y, geom
          FROM public.slide_area, public.project, public.admin_area, public.working_circle, public.reservoir, public.watershed, public.forest_district, public.basin
          WHERE project_no = project_id AND county_no = county_id AND town_no = town_id AND workingcircle_no = workingcircle_id AND reservoir_no = reservoir_id AND water_no = water_id AND forest_no = forest_id AND basin_no = basin_id
          GROUP BY slide_id, geom, project_date, workingcircle_name, forest_name, county_name, town_name, reservoir_name, water_name, basin_name, area, centroid_x, centroid_y) AS T;
END IF;
END$$
