-- Enable PostGIS (includes raster)
CREATE EXTENSION postgis;
-- Enable Topology
CREATE EXTENSION postgis_topology;
-- fuzzy matching needed for Tiger
CREATE EXTENSION fuzzystrmatch;
-- Enable US Tiger Geocoder
CREATE EXTENSION postgis_tiger_geocoder;

-- Delete old tables
DROP TABLE IF EXISTS public.slide_area;
DROP TABLE IF EXISTS public.project;

-- Create table admin_area, primary key is (county_id, town_id)
DROP TABLE IF EXISTS public.admin_area;
CREATE TABLE public.admin_area
(
  county_id varchar(5) NOT NULL,
  town_id varchar(10) NOT NULL,
  county_name varchar(50),
  town_name varchar(50),
  CONSTRAINT ADMINPK PRIMARY KEY (county_id, town_id)
);
ALTER TABLE public.admin_area OWNER TO postgres;

-- Create table working_circle, primary key is workingcircle_id
DROP TABLE IF EXISTS public.working_circle;
CREATE TABLE public.working_circle
(
  workingcircle_id varchar(2) NOT NULL,
  workingcircle_name varchar(50),
  CONSTRAINT WORKPK PRIMARY KEY (workingcircle_id)
);
ALTER TABLE public.working_circle OWNER TO postgres;

-- Create table watershed, primary key is water_id
DROP TABLE IF EXISTS public.watershed;
CREATE TABLE public.watershed
(
  water_id varchar(10) NOT NULL,
  water_name varchar(30),
  CONSTRAINT WARTERPK PRIMARY KEY (water_id)
);
ALTER TABLE public.watershed OWNER TO postgres;

-- Create table forest_district, primary key is forest_id
DROP TABLE IF EXISTS public.forest_district;
CREATE TABLE public.forest_district
(
  forest_id varchar(2) NOT NULL,
  forest_name varchar(50),
  CONSTRAINT FORESTPK PRIMARY KEY (forest_id)
);
ALTER TABLE public.forest_district OWNER TO postgres;

-- Create table reservoir, primary key is reservoir_id
DROP TABLE IF EXISTS public.reservoir;
CREATE TABLE public.reservoir
(
  reservoir_id varchar(2) NOT NULL,
  reservoir_name varchar(12),
  CONSTRAINT RESERVPK PRIMARY KEY (reservoir_id)
);
ALTER TABLE public.reservoir OWNER TO postgres;

-- Create table basin, primary key is basin_id
DROP TABLE IF EXISTS public.basin;
CREATE TABLE public.basin
(
  basin_id varchar(5) NOT NULL,
  basin_name varchar(16),
  CONSTRAINT BASINPK PRIMARY KEY (basin_id)
);
ALTER TABLE public.basin OWNER TO postgres;

-- Create sequence for table project
DROP SEQUENCE IF EXISTS public.project_id_seq;
CREATE SEQUENCE public.project_id_seq START 1;

-- Create table project, primary key is project_id
CREATE TABLE public.project
(
  project_id integer NOT NULL DEFAULT nextval('project_id_seq'::regclass),
  project_date date,
  CONSTRAINT PROJECTPK PRIMARY KEY (project_id)
);
ALTER TABLE public.project OWNER TO postgres;

-- Create sequence for table map5000
DROP SEQUENCE IF EXISTS public.map_id_id_seq;
CREATE SEQUENCE public.map_id_id_seq START 1;

-- Create table map5000, primary key is map_id_id
CREATE TABLE public.map5000
(
  map_id_id integer NOT NULL DEFAULT nextval('map_id_id_seq'::regclass),
  map_id numeric,
  CONSTRAINT MAPPK PRIMARY KEY (map_id_id)
);
ALTER TABLE public.map5000 OWNER TO postgres;

-- Create sequence for table slide_area
DROP SEQUENCE IF EXISTS public.slide_id_seq;
CREATE SEQUENCE public.slide_id_seq START 1; 

/*
Create table slide_area, primary key is slide_id
Foreign keys are (county_no, town_no), referenced from admin_area(county_id, town_id)
                 project_no, referenced from project(project_id)
                 map5000_no, referenced from map5000(map_id_id)
                 workingcircle_no, referenced from working_circle(workingcircle_id)
                 reservoir_no, referenced from reservoir(reservoir_id)
                 water_no, referenced from watershed(water_id)
                 forest_no, referenced from forest_district(forest_id)
                 basin_no, referenced from basin(basin_id)*/

CREATE TABLE public.slide_area
(
  slide_id integer NOT NULL DEFAULT nextval('slide_id_seq'::regclass),
  centroid_x numeric,
  centroid_y numeric,
  area numeric,
  geom geometry(MultiPolygon,3826),
  project_no integer,
  map5000_no integer,
  input_date date,
  remarks varchar(30),
  map_name varchar(50),
  county_no varchar(5),
  town_no varchar(10),
  workingcircle_no varchar(2),
  reservoir_no varchar(12),
  water_no varchar(10),
  forest_no varchar(2),
  basin_no varchar(5),
  
  CONSTRAINT SLIDEPK 
    PRIMARY KEY (slide_id),
  CONSTRAINT SLIDEFK_ADMIN
    FOREIGN KEY (county_no, town_no) REFERENCES admin_area(county_id, town_id),    
  CONSTRAINT SLIDEFK_PROJECT 
    FOREIGN KEY (project_no) REFERENCES project(project_id),
  CONSTRAINT SLIDEFK_MAP 
    FOREIGN KEY (map5000_no) REFERENCES map5000(map_id_id),
  CONSTRAINT SLIDEFK_WORK  
    FOREIGN KEY (workingcircle_no) REFERENCES working_circle(workingcircle_id),
  CONSTRAINT SLIDEFK_RESERV
    FOREIGN KEY (reservoir_no) REFERENCES reservoir(reservoir_id),
  CONSTRAINT SLIDEFK_WATER    
    FOREIGN KEY (water_no) REFERENCES watershed(water_id),
  CONSTRAINT SLIDEFK_FOREST
    FOREIGN KEY (forest_no) REFERENCES forest_district(forest_id),
  CONSTRAINT SLIDEFK_BASIN
    FOREIGN KEY (basin_no) REFERENCES basin(basin_id)
);
ALTER TABLE public.slide_area OWNER TO postgres;
