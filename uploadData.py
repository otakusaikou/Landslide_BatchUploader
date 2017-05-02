#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import psycopg2
import glob
import datetime
import numpy as np


def initDB(host, port, user, dbName):
    """Initialize landslide database"""
    # Replace and create a new database with PostGIS extension
    print "Initialize landslide database..."
    cmdStr = "psql -h %s -p %s -U %s -c \"DROP DATABASE IF EXISTS %s;\"" \
        % (host, port, user, dbName)
    os.popen(cmdStr)

    cmdStr = "psql -h %s -p %s -U %s -c \"CREATE DATABASE %s;\"" \
        % (host, port, user, dbName)
    os.popen(cmdStr)

    cmdStr = "psql -h %s -p %s -U %s -d %s -f sql/dbinit.sql" \
        % (host, port, user, dbName)
    os.popen(cmdStr)


def removeRef(conn, layers):
    """Drop unused tables"""
    cur = conn.cursor()     # Get cursor object of database connection

    # Clean old identity layer tables
    sql = ";".join(map(
        lambda x: "DROP TABLE IF EXISTS public.%s" % x.split(".")[0],
        layers + ["inputData", "result"]))

    cur.execute(sql)
    conn.commit()


def loadRef(conn, rootDir, host, port, user, dbName):
    """Upload identity layers to database"""
    # Check identity layers exist
    identitylyr = os.path.join(rootDir, "reference_data", "identityLayer")

    if not os.path.exists(identitylyr) or not glob.glob(
            identitylyr + "/*.shp"):
        print "Identity layer is missing!"
        return None

    # Get identity layer list
    os.chdir(identitylyr)
    layers = glob.glob("*.shp")

    # Remove old reference tables
    removeRef(conn, layers)

    # Import identity layers
    for f in layers:
        print "Import identity layer '%s' to database..." % f
        cmdStr = "shp2pgsql -s 3826 -c -D -I -W big5 %s %s | psql -h %s -p" \
            "%s -d %s -U %s" % (f, f.split(".")[0], host, port, dbName, user)
        os.popen(cmdStr)

    os.chdir(rootDir)

    return layers


def uploadShp(conn, csvFile, rootDir, host, port, user, dbName):
    """Upload target landslide data to database"""
    cur = conn.cursor()     # Get cursor object of database connection

    # Upload identity layers
    layers = loadRef(conn, rootDir, host, port, user, dbName)
    if not layers:
        return None

    os.chdir(os.path.join(rootDir, "shp"))

    # Read information from csv file
    data = np.genfromtxt(csvFile, delimiter=",", dtype=object)
    tmpName, mapName, remarks, projDate, inputDate = map(
        lambda x: x.flatten(), np.hsplit(data, 5))
    tmpName += ".shp"

    # Check if the shapefile exists
    for i in range(len(tmpName)):
        if not os.path.exists(tmpName[i]):
            print "Cannot find shapefile: '%s', make sure the path and file" \
                " name is correct." % tmpName[i]
            continue

        # Import shapefile to database as a template table
        sql = "DROP TABLE IF EXISTS inputData;"
        cur.execute(sql)
        conn.commit()

        print "Import shapefile '%s' to database..." % tmpName[i]
        cmdStr = "shp2pgsql -s 3826 -c -D -I -W big5 %s inputData | psql -h " \
            "%s -p %s -d %s -U %s" % (tmpName[i], host, port, dbName, user)
        os.popen(cmdStr)

        # Insert project date column to input table
        sql = "ALTER TABLE inputData ADD COLUMN tmp_date date;" + \
            "UPDATE inputData SET tmp_date = '%s';" % projDate[i]
        cur.execute(sql)
        conn.commit()

        # Perform identity analysis
        print "Perform identity analysis..."
        cmdStr = "psql -h %s -p %s -U %s -d %s -f" \
            "../sql/zonalSplit.sql" % (host, port, user, dbName)
        os.popen(cmdStr)

        # Insert filing date, source file name and remarks
        sql = \
            "ALTER TABLE result ADD COLUMN map_name varchar(50);" + \
            "ALTER TABLE result ADD COLUMN remarks varchar(50);" + \
            "ALTER TABLE result ADD COLUMN input_date date;" + \
            "UPDATE result SET map_name = '%s';" % mapName[i] + \
            "UPDATE result SET remarks= '%s';" % remarks[i] + \
            "UPDATE result SET input_date= '%s';" % inputDate[i]
        cur.execute(sql)
        conn.commit()

        # Update each table in database
        print "Update the database..."
        cmdStr = "psql -h %s -p %s -U %s -d %s -f ../sql/datain.sql" \
            % (host, port, user, dbName)
        os.popen(cmdStr)

    # Remove unnecessary table
    print "Remove unnecessary table..."
    removeRef(conn, layers)


def main():
    tStart = datetime.datetime.now()

    # Define database connection parameters
    host = "localhost"
    port = "5432"
    dbName = "landslide"
    user = "postgres"

    csvFile = "f6.csv"  # Shapefile information
    rootDir = os.getcwd()

    # Ask user whether to reinitialize the database
    flag = raw_input("Initialize database? (Y/N) ").lower()
    while flag not in ["yes", "no", "y", "n"]:
        flag = raw_input(
            "Invalid selection (You should input 'Y' or 'N') ").lower()

    if flag in ["Y", "y", "Yes", "yes"]:
        initDB(host, port, user, dbName)

    # Connect to database
    try:
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' port='%s'"
                                % (dbName, user, host, port))
    except psycopg2.OperationalError:
        print "Unable to connect to the database."
        return -1

    uploadShp(conn, csvFile, rootDir, host, port, user, dbName)
    conn.close()

    tEnd = datetime.datetime.now()
    print "Works done! It took %f sec" % (tEnd - tStart).total_seconds()

    return 0


if __name__ == '__main__':
    main()
