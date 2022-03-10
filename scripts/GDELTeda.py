'''GDELTeda.py
Project: WGU Data Management/Analytics Undergraduate Capstone
Richard Smith
August 2021

  Class for collecting Pymongo and Pandas operations to automate EDA on
subsets of GDELT records (Events/Mentions, GKG, or joins).

  Basic use should be by import and implementation within an IDE, or by editing
section # C00 and running this script directly.

  Primary member functions include descriptive docstrings for their intent and
use.

  WARNING: project file operations are based on relative pathing from the
'scripts' directory this Python script is located in, given the creation of
directories 'GDELTdata' and 'EDAlogs' parallel to 'scripts' upon first
GDELTbase and GDELTeda class initializations. If those directories are not
already present, a fallback method for string-literal directory reorientation
may be found in '__init__()' at this tag: # A02b - Project directory path.
Specification for any given user's main project directory should be made for
that os.chdir() call.
See also GDELTbase.py, tag # A01a - backup path specification, as any given
user's project directory must be specified there, also.

Contents:
  A00 - GDELTeda
    A01 - shared class data
    A02 - __init__ with instanced data
      A02a - Project directory maintenance
      A02b - Project directory path specification
      Note: Specification at A02b should be changed to suit a user's desired
              directory structure, given their local filesystem.
  B00 - class methods
    B01 - batchEDA()
    B02 - eventsBatchEDA()
    B03 - mentionsBatchEDA()
    B04 - gkgBatchEDA()
      Note: see GDELTedaGKGhelpers.py for helper function code & docs
    B05 - realtimeEDA()
    B06 - loopEDA()
  C00 - main w/ testing
    C01 - previously-run GDELT realtime EDA testing
'''

import json
import multiprocessing
import numpy as np
import os
import pandas as pd
import pymongo
import shutil
import wget
from datetime import datetime, timedelta, timezone
from GDELTbase import GDELTbase
from GDELTedaGKGhelpers import GDELTedaGKGhelpers
from pandas_profiling import ProfileReport
from pprint import pprint as pp
from time import time, sleep
from urllib.error import HTTPError
from zipfile import ZipFile as zf

# A00
class GDELTeda:
  '''Collects Pymongo and Pandas operations for querying GDELT records
subsets and performing semi-automated EDA.

Shared class data:
-----------------

logPath - dict
  Various os.path objects for EDA log storage.

configFilePaths - dict
  Various os.path objects for pandas_profiling.ProfileReport
 configuration files, copied to EDA log storage directories upon
 __init__, for use in report generation.
   
Instanced class data:
--------------------

gBase - GDELTbase instance
  Used for class member functions, essential for realtimeEDA().


Class methods:
-------------

batchEDA()
eventsBatchEDA()
mentionsBatchEDA()
gkgBatchEDA()
realtimeEDA()
loopEDA()

Helper functions from GDELTedaGKGhelpers.py used in gkgBatchEDA():
  pullMainGKGcolumns()
  applyDtypes()
  convertDatetimes()
  convertGKGV15Tone()
  mainReport()
  locationsReport()
  countsReport()
  themesReport()
  personsReport()
  organizationsReport()
  '''

  # A01 - shared class data

  #  These paths are set relative to the location of this script, one directory
  # up and in 'EDAlogs' parallel to the script directory, which can be named
  # arbitrarily.
  logPath = {}
  logPath['base'] = os.path.join(os.path.abspath(__file__),
                                 os.path.realpath('..'),
                                 'EDAlogs')
  logPath['events'] = {}
  logPath['events'] = {
    'table' : os.path.join(logPath['base'], 'events'),
    'batch' : os.path.join(logPath['base'], 'events', 'batch'),
    'realtime' : os.path.join(logPath['base'], 'events', 'realtime'),
    }
  logPath['mentions'] = {
    'table' : os.path.join(logPath['base'], 'mentions'),
    'batch' : os.path.join(logPath['base'], 'mentions', 'batch'),
    'realtime' : os.path.join(logPath['base'], 'mentions', 'realtime'),
    }
  logPath['gkg'] = {
    'table' : os.path.join(logPath['base'], 'gkg'),
    'batch' : os.path.join(logPath['base'], 'gkg', 'batch'),
    'realtime' : os.path.join(logPath['base'], 'gkg', 'realtime'),
    }

  #   Turns out, the following isn't the greatest way of keeping track
  # of each configuration file. It's easiest to just leave them in the
  # exact directories where ProfileReport.to_html() is aimed (via
  # os.chdir()), since it's pesky maneuvering outside parameters into
  # multiprocessing Pool.map() calls.
  #   Still, these can and are used in realtimeEDA(), since the size of
  # just the most recent datafiles should permit handling them without
  # regard for Pandas DataFrame RAM impact (it's greedy, easiest method
  # for mitigation is multiprocessing threads, that shouldn't be
  # necessary for realtimeEDA()).
  #   Regardless, all these entries are for copying ProfileReport config
  # files to their appropriate directories for use, given base-copies
  # present in the 'scripts' directory. Those base copies may be edited
  # in 'scripts', since each file will be copied from there.
  configFilePaths = {}
  configFilePaths['events'] = {
    'batch'    : os.path.join(logPath['base'],
                              os.path.realpath('..'),
                              'scripts',
                              "GDELTeventsEDAconfig_batch.yaml"),
    'realtime' : os.path.join(logPath['base'],
                              os.path.realpath('..'),
                              'scripts',
                              "GDELTeventsEDAconfig_realtime.yaml"),
    }

  configFilePaths['mentions'] = {
    'batch'    : os.path.join(logPath['base'],
                              os.path.realpath('..'),
                              'scripts',
                              "GDELTmentionsEDAconfig_batch.yaml"),
    'realtime' : os.path.join(logPath['base'],
                              os.path.realpath('..'),
                              'scripts',
                              "GDELTmentionsEDAconfig_realtime.yaml"),
    }
  configFilePaths['gkg'] = {}
  configFilePaths['gkg']['batch'] = {
    'main'          : os.path.join(logPath['base'],
                                  os.path.realpath('..'),
                                  'scripts',
                                  "GDELTgkgMainEDAconfig_batch.yaml"),
    'locations'     : os.path.join(logPath['base'],
                                  os.path.realpath('..'),
                                  'scripts',
                               "GDELTgkgLocationsEDAconfig_batch.yaml"),
    'counts'        : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                                  "GDELTgkgCountsEDAconfig_batch.yaml"),
    'themes'        : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                                  "GDELTgkgThemesEDAconfig_batch.yaml"),
    'persons'       : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                                 "GDELTgkgPersonsEDAconfig_batch.yaml"),
    'organizations' : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                           "GDELTgkgOrganizationsEDAconfig_batch.yaml"),
    }
  configFilePaths['gkg']['realtime'] = {
    'main'          : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                                   "GDELTgkgMainEDAconfig_realtime.yaml"),
    'locations'     : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                                   "GDELTgkgLocationsEDAconfig_realtime.yaml"),
    'counts'        : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                                   "GDELTgkgCountsEDAconfig_realtime.yaml"),
    'themes'        : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                                   "GDELTgkgThemesEDAconfig_realtime.yaml"),
    'persons'       : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                                   "GDELTgkgPersonsEDAconfig_realtime.yaml"),
    'organizations' : os.path.join(logPath['base'],
                                   os.path.realpath('..'),
                                   'scripts',
                               "GDELTgkgOrganizationsEDAconfig_realtime.yaml"),
    }

  # A02
  def __init__(self, tableList = ['events', 'mentions', 'gkg']):
    '''GDELTeda class initialization, takes a list of GDELT tables to
perform EDA on. Instantiates a GDELTbase() instance for use by class
methods and checks for presence of EDAlogs directories, creating them if
they aren't present, and copying all ProfileReport-required config files
to their applicable directories.

Parameters:
----------

tableList - list of strings, default ['events','mentions','gkg']
   Controls detection and creation of .../EDALogs/... subdirectories for
 collection of Pandas Profiling ProfileReport HTML EDA document output.
   Also controls permission for class member functions to perform
 operations on tables specified by those functions' tableList parameters
 as a failsafe against a lack of project directories required for those
 operations, specifically output of HTML EDA documents.

output:
------

  Produces exhaustive EDA for GDELT record subsets for specified tables
 through Pandas Profiling ProfileReport-output HTML documents.
  All procedurally automated steps towards report generation are shown
 in console output during script execution.
    '''
    # instancing tables for operations to be passed to member functions
    self.tableList = tableList

    print("Instantiating GDELTeda...\n")
    self.gBase = GDELTbase()

    if 'events' not in tableList and \
       'mentions' not in tableList and \
       'gkg' not in tableList:
      print("Error! 'tableList' values do not include a valid GDELT table.",
            "\nPlease use one or more of 'events', 'mentions', and/or 'gkg'.")
    
    # instancing trackers for realtimeEDA() and loopEDA()
    self.realtimeStarted = False
    self.realtimeLooping = False
    self.realtimeWindow = 0
    self.lastRealDatetime = ''
    self.nextRealDatetime = ''


    # A02a - Project EDA log directories confirmation and/or creation, and
    # Pandas Profiling ProfileReport configuration file copying from 'scripts'
    # directory.
    print("  Checking log directory...")
    if not os.path.isdir(self.logPath['base']):
      print("    Doesn't exist! Making...")
      # A02b - Project directory path
      #   For obvious reasons, any user of this script should change this
      # string to suit their needs. The directory described with this string
      # should be one directory above the location of the 'scripts' directory
      # this file should be in. If this file is not in 'scripts', unpredictable
      # behavior may occur, and no guarantees of functionality are intended for
      # such a state.
      os.chdir('C:\\Users\\urf\\Projects\\WGU capstone')
      os.mkdir(self.logPath['base'])
    for table in tableList:
      # switch to EDAlogs directory
      os.chdir(self.logPath['base'])
      # Branch: table subdirectories not found, create all
      if not os.path.isdir(self.logPath[table]['table']):
        print("Did not find .../EDAlogs/", table, "...")
        print("  Creating .../EDAlogs/", table, "...")
        os.mkdir(self.logPath[table]['table'])
        os.chdir(self.logPath[table]['table'])
        print("  Creating .../EDAlogs/", table, "/batch")
        os.mkdir(self.logPath[table]['batch'])
        print("  Creating .../EDAlogs/", table, "/realtime")
        os.mkdir(self.logPath[table]['realtime'])
        os.chdir(self.logPath[table]['realtime'])
      #   Branch: table subdirectories found, create batch/realtime directories
      # if not present.
      else:
        print("  Found .../EDAlogs/", table,"...")
        os.chdir(self.logPath[table]['table'])
        if not os.path.isdir(self.logPath[table]['batch']):
          print("    Did not find .../EDAlogs/", table, "/batch , creating...")
          os.mkdir(self.logPath[table]['batch'])
        if not os.path.isdir(self.logPath[table]['realtime']):
          print("    Did not find .../EDAlogs/", table, "/realtime , creating...")
          os.mkdir(self.logPath[table]['realtime'])
          os.chdir(self.logPath[table]['realtime'])
      # Copying pandas_profiling.ProfileReport configuration files
      print("  Copying configuration files...\n")
      if table == 'gkg':
        #   There's a lot of these, but full normalization of GKG is
        # prohibitively RAM-expensive, so reports need to be generated for
        # both the main columns and the main columns normalized for each
        # variable-length subfield.
        shutil.copy(self.configFilePaths[table]['realtime']['main'],
                    self.logPath[table]['realtime'])
        shutil.copy(self.configFilePaths[table]['realtime']['locations'],
                    self.logPath[table]['realtime'])
        shutil.copy(self.configFilePaths[table]['realtime']['counts'],
                    self.logPath[table]['realtime'])
        shutil.copy(self.configFilePaths[table]['realtime']['themes'],
                    self.logPath[table]['realtime'])
        shutil.copy(self.configFilePaths[table]['realtime']['persons'],
                    self.logPath[table]['realtime'])
        shutil.copy(self.configFilePaths[table]['realtime']['organizations'],
                    self.logPath[table]['realtime'])
        os.chdir(self.logPath[table]['batch'])
        shutil.copy(self.configFilePaths[table]['batch']['main'],
                    self.logPath[table]['batch'])
        shutil.copy(self.configFilePaths[table]['batch']['locations'],
                    self.logPath[table]['batch'])
        shutil.copy(self.configFilePaths[table]['batch']['counts'],
                    self.logPath[table]['batch'])
        shutil.copy(self.configFilePaths[table]['batch']['themes'],
                    self.logPath[table]['batch'])
        shutil.copy(self.configFilePaths[table]['batch']['persons'],
                    self.logPath[table]['batch'])
        shutil.copy(self.configFilePaths[table]['batch']['organizations'],
                    self.logPath[table]['batch'])
      else:
        shutil.copy(self.configFilePaths[table]['realtime'],
                    self.logPath[table]['realtime'])
        os.chdir(self.logPath[table]['batch'])
        shutil.copy(self.configFilePaths[table]['batch'],
                    self.logPath[table]['batch'])


  # B00 - class methods

  # B01
  def batchEDA(self, tableList = ['events','mentions','gkg']):
    '''Reshapes and re-types GDELT records for generating Pandas
Profiling ProfileReport()-automated, simple EDA reports from Pandas
DataFrames, from MongoDB-query-cursors.

WARNING: extremely RAM, disk I/O, and processing intensive. Be aware of
what resources are available for these operations at runtime.

  Relies on Python multiprocessing.Pool.map() calls against class member
functions eventsBatchEDA() and mentionsBatchEDA(), and a regular call on
gkgBatchEDA(), which uses multiprocessing.Pool.map() calls within it.

Parameters:
----------

tableList - list of strings, default ['events','mentions','gkg']
  Permits limiting analysis to one or more tables.

Output:
------

  Displays progress through the function's operations via console output
 while producing Pandas Profiling ProfileReport.to_file() html documents
 for 
    '''
    if tableList != self.tableList:
      print("\n  Error: this GDELTeda object may have been initialized\n",
            "        without checking for the presence of directories\n",
            "        required for this function's operations.\n",
            "        Please check GDELTeda parameters and try again.")

    for table in tableList:
      
      print("\n------------------------------------------------------------\n")

      print("Executing batch EDA on GDELT table", table, "records...")
      
      #  WARNING: RAM, PROCESSING, and DISK I/O INTENSIVE
      #  Events and Mentions are both much easier to handle than GKG, so
      # they're called in their own collective function threads with
      # multiprocessing.Pool(1).map().
      if table == 'events':
        os.chdir(self.logPath['events']['batch'])
        pool = multiprocessing.Pool(1)
        eventsReported = pool.map(self.eventsBatchEDA(), ['batch'])
        pool.close()
        pool.join()
      if table == 'mentions':
        os.chdir(self.logPath['mentions']['batch'])
        pool = multiprocessing.Pool(1)
        mentionsReported = pool.map(self.mentionsBatchEDA(), ['batch'])
        pool.close()
        pool.join()
      if table == 'gkg':
        #   Here's the GKG bottleneck! Future investigation of parallelization
        # improvements may yield gains here, as normalization of all subfield
        # and variable-length measures is very RAM expensive, given the
        # expansion in records required.
        #   So, current handling of GKG subfield and variable-length measures
        # is isolating most operations in their own process threads within
        # gkgBatchEDA() execution, forcing deallocation of those resources upon
        # each Pool.close(), as with Events and Mentions table operations above
        # which themselves do not require any additional subfield handling.
        os.chdir(self.logPath['gkg']['batch'])
        self.gkgBatchEDA()

  # B02
  def eventsBatchEDA(mode):
    '''Performs automatic EDA on GDELT Events record subsets. See
 function batchEDA() for "if table == 'events':" case handling and how
 this function is invoked as a multiprocessing.Pool.map() call, intended
 to isolate its RAM requirements for deallocation upon Pool.close().

  In its current state, this function can handle collections of GDELT
Events records up to at least the size of the batch EDA test subset used
in this capstone project, the 30 day period from 05/24/2020 to
06/22/2020.

Parameters:
----------
  
mode - arbitrary
  This parameter is included to meet Python multiprocessing.Pool.map()
 function requirements. As such, it is present only to receive a
 parameter determined by map(), e.g. one iteration of the function will
 execute.

Output:
------

  Console displays progress through steps with time taken throughout,
and function generates EDA profile html documents in appropriate project
directories.
    '''
    columnNames = [
      'GLOBALEVENTID',
      'Actor1Code',
      'Actor1Name',
      'Actor1CountryCode',
      'Actor1Type1Code',
      'Actor1Type2Code',
      'Actor1Type3Code',
      'Actor2Code',
      'Actor2Name',
      'Actor2CountryCode',
      'Actor2Type1Code',
      'Actor2Type2Code',
      'Actor2Type3Code',
      'IsRootEvent',
      'EventCode',
      'EventBaseCode',
      'EventRootCode',
      'QuadClass',
      'AvgTone',
      'Actor1Geo_Type',
      'Actor1Geo_FullName',
      'Actor1Geo_Lat',
      'Actor1Geo_Long',
      'Actor2Geo_Type',
      'Actor2Geo_FullName',
      'Actor2Geo_Lat',
      'Actor2Geo_Long',
      'ActionGeo_Type',
      'ActionGeo_FullName',
      'ActionGeo_Lat',
      'ActionGeo_Long',
      'DATEADDED',
      'SOURCEURL',
      ]
    columnTypes = {
      'GLOBALEVENTID' : type(1),
      'Actor1Code': pd.StringDtype(),
      'Actor1Name': pd.StringDtype(),
      'Actor1CountryCode': pd.StringDtype(),
      'Actor1Type1Code' : pd.StringDtype(),
      'Actor1Type2Code' : pd.StringDtype(),
      'Actor1Type3Code' : pd.StringDtype(),
      'Actor2Code': pd.StringDtype(),
      'Actor2Name': pd.StringDtype(),
      'Actor2CountryCode': pd.StringDtype(),
      'Actor2Type1Code' : pd.StringDtype(),
      'Actor2Type2Code' : pd.StringDtype(),
      'Actor2Type3Code' : pd.StringDtype(),
      'IsRootEvent': type(True),
      'EventCode': pd.StringDtype(),
      'EventBaseCode': pd.StringDtype(),
      'EventRootCode': pd.StringDtype(),
      'QuadClass': type(1),
      'AvgTone': type(1.1),
      'Actor1Geo_Type': type(1),
      'Actor1Geo_FullName': pd.StringDtype(),
      'Actor1Geo_Lat': pd.StringDtype(),
      'Actor1Geo_Long': pd.StringDtype(),
      'Actor2Geo_Type': type(1),
      'Actor2Geo_FullName': pd.StringDtype(),
      'Actor2Geo_Lat': pd.StringDtype(),
      'Actor2Geo_Long': pd.StringDtype(),
      'ActionGeo_Type': type(1),
      'ActionGeo_FullName': pd.StringDtype(),
      'ActionGeo_Lat': pd.StringDtype(),
      'ActionGeo_Long': pd.StringDtype(),
      'DATEADDED' : pd.StringDtype(),
      'SOURCEURL': pd.StringDtype(),
      }
    timecheckG = time()
    print("  Creating another thread-safe connection to MongoDB...")
    localDb = {}
    localDb['client'] = pymongo.MongoClient()
    localDb['database'] = localDb['client'].capstone
    localDb['collection'] = localDb['database'].GDELT.events
  
    configFilePath = os.path.join(os.path.abspath(__file__),
                                  "GDELTeventsEDAconfig_batch.yaml"),
      
    datetimeField = "DATEADDED"
    datetimeFormat = "%Y-%m-%dT%H:%M:%S.000000Z"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    print("  Pulling events records (long wait)... ", end = '')
    eventsDF = pd.DataFrame.from_records(
                list(localDb['collection'].find(projection = {"_id" : False},
                                                allow_disk_use=True,
                                                no_cursor_timeout = True)),
                columns = columnNames,
                )
    print("    Complete!( %0.3f s )" % (float(time())-float(timecheckG)))

    timecheckG = time()
    print("  Setting dtypes... ", end='')
    eventsDF = eventsDF.astype(dtype = columnTypes, copy = False)
    print("    Complete!( %0.3f s )" % (float(time())-float(timecheckG)))

    print("  Converting datetimes...", end = '')
    eventsDF[datetimeField] = pd.to_datetime(eventsDF[datetimeField],
                                            format = datetimeFormat)
    print("    Complete!( %0.3f s )" % (float(time())-float(timecheckG)))

    print("\n  Events records DataFrame .info():\n")
    print(eventsDF.info())

    edaDates = "".join([
      eventsDF[datetimeField].min().strftime(strftimeFormat),"_to_",
      eventsDF[datetimeField].max().strftime(strftimeFormat),
      ])
    edaLogName = "".join(["GDELT_events_EDA_", edaDates,".html"])
    timecheckG = time()

    print("    Complete!( %0.3f s )" % (float(time())-float(timecheckG)))
    timecheckG = time()
    print("  File output:", edaLogName, "\n")

    print("  Generating events 'batch' EDA report...")

    eventsProfile = ProfileReport(eventsDF, config_file = configFilePath)
    eventsProfile.to_file(edaLogName)
    del eventsDF
    del eventsProfile
    print("    Complete!( %0.fd s )" % (float(time())-float(timecheckG)))
    print("All Events EDA operations complete. Please check EDAlogs",
          "directories for any resulting Events EDA profile reports.")
    return True


  # B03
  def mentionsBatchEDA(mode):
    '''Performs automatic EDA on GDELT Mentions record subsets. See
 function batchEDA() for "if table == 'mentions':" case handling and how
 this function is invoked as a multiprocessing.Pool.map() call, intended
 to isolate its RAM requirements for deallocation upon Pool.close().
  
  In its current state, this function can handle collections of GDELT
Mentions records up to at least the size of the batch EDA test subset
used in this capstone project, the 30 day period from 05/24/2020 to
06/22/2020.

Parameters:
----------
  
mode - arbitrary
  This parameter is included to meet Python multiprocessing.Pool.map()
 function requirements. As such, it it present only to receive a
 parameter determined by imap(chunksize = 1), e.g. one iteration of the
 function will execute.
  
Output:
------

  Console displays progress through steps with time taken throughout,
and function generates EDA profile html documents in appropriate project
directories.
    '''
    print("  Creating new thread-safe connection to MongoDB...")
    columnNames = [
      'GLOBALEVENTID',
      'EventTimeDate',
      'MentionTimeDate',
      'MentionType',
      'MentionSourceName',
      'MentionIdentifier',
      'InRawText',
      'Confidence',
      'MentionDocTone',
      ]
    columnTypes = {
      'GLOBALEVENTID' : type(1),
      'EventTimeDate' : pd.StringDtype(),
      'MentionTimeDate' : pd.StringDtype(),
      'MentionType' : pd.StringDtype(),
      'MentionSourceName' : pd.StringDtype(),
      'MentionIdentifier' : pd.StringDtype(),
      'InRawText' : type(True),
      'Confidence' : type(1),
      'MentionDocTone' : type(1.1),
      }

    localDb = {}
    localDb['client'] = pymongo.MongoClient()
    localDb['database'] = localDb['client'].capstone
    localDb['collection'] = localDb['database'].GDELT.mentions

    configFileName = "GDELTmentionsEDAconfig_batch.yaml"
      
    datetimeField01 = "MentionTimeDate"
    datetimeField02 = "EventTimeDate"
    datetimeFormat = "%Y-%m-%dT%H:%M:%S.000000Z"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    print("\n  Pulling mentions records (long wait)...")
    tableDF = pd.DataFrame.from_records(
                list(localDb['collection'].find(projection = {"_id" : False},
                                                allow_disk_use=True,
                                                no_cursor_timeout = True)),
                columns = columnNames,
                )
    print("    Complete!")

    print("  Setting dtypes...")
    tableDF = tableDF.astype(dtype = columnTypes, copy = False)
    print("    Complete!")

    print("  Converting datetimes...")
    tableDF[datetimeField01] = pd.to_datetime(tableDF[datetimeField01],
                                            format = datetimeFormat)
    tableDF[datetimeField02] = pd.to_datetime(tableDF[datetimeField02],
                                            format = datetimeFormat)
    print("    Complete!")

    print("  Mentions records DataFrame .info():")
    print(tableDF.info())
    edaDates = "".join([
      tableDF[datetimeField01].min().strftime(strftimeFormat),"_to_",
      tableDF[datetimeField01].max().strftime(strftimeFormat),
      ])
    edaLogName = "".join(["GDELT_mentions_EDA_", edaDates,".html"])
    print("  File output:", edaLogName, "\n")

    print("\n  Generating mentions 'batch' EDA report...")
    profile = ProfileReport(tableDF, config_file= configFileName)
    profile.to_file(edaLogName)
    print("\n    Complete!")
    return True


  # B04
  def gkgBatchEDA(self):
    '''Performs automatic EDA on GDELT Global Knowledge Graph (GKG)
record subsets.

   Makes use of these helper functions for multiprocessing.Pool.map()
 calls, from GDELTedaGKGhelpers. a separate file as part of ensuring
 compatibility with this class's Python.multiprocessing calls (all 'AXX'
 tags are for 'find' use in GDELTedaGKGhelpers.py):

  A02 - pullMainGKGcolumns
  A03 - applyDtypes
  A04 - convertDatetimes
  A05 - convertGKGV15Tone
  A06 - mainReport
  A07 - locationsReport
  A08 - countsReport
  A09 - themesReport
  A10 - personsReport
  A11 - organizationsReport

  The intent behind this implementation is to reduce the amount of total
RAM required for all operations, as .close() upon appropriate process
pools should result in deallocation of their memory structures, which
just isn't going to be forced otherwise due to Pandas and Python memory
handling.

  Well-known issues with underlying treatment of allocation and
deallocation of DataFrames, regardless of whether all references to a
DataFrame are passed to 'del' statements, restrict completion of the
processing necessary for normalization of all GKG columns, which is
necessary for execution of EDA on those columns. The apparent RAM
requirements for those operations on the batch test GKG data set are not
mitigable under these hardware circumstances, barring further subset of
the data into small enough component pieces with each their own EDA
profile, which could not be converted to batch EDA without processing
outside the scope of this capstone project.

  The uncommented code in this function represent a working state which
can produce full batch EDA on at least the primary information-holding
GKG columns, but not for the majority of variable-length and subfielded
columns. V1Locations batch EDA has been produced from at least one
attempt, but no guarantee of its error-free operation on similarly-sized
subsets of GDELT GKG records is intended or encouraged.
  
Output:
------

  Console displays progress through steps with time taken throughout,
and function generates EDA profile html documents in appropriate project
directories.
    '''
    timecheck = time()
    print("  Pulling non-variable-length GKG columns...")
    pool = multiprocessing.Pool(1)
    #   For pullMainGKGcolumns documentation, See GDELTedaGKGhelpers.py,
    # 'find' tag '# A02'
    tableDF = pool.map(GDELTedaGKGhelpers.pullMainGKGcolumns, ['batch'])
    pool.close()
    pool.join()
    print("    Records acquired. ( %0.3f s )" % (float(time())-float(timecheck)))
    print("\n  GKG records DataFrame .info():")
    tableDF = tableDF.pop()
    pp(tableDF.info())

    timecheck = time()
    print("\n  Setting dtypes...")
    #   This only sets 'GKGRECORDID', 'V21DATE', 'V2SourceCommonName',
    # and 'V2DocumentIdentifier' dtypes to pd.StringDtype()
    pool = multiprocessing.Pool(1)
    #   For applyDtypes documentation, See GDELTedaGKGhelpers.py, 'find'
    # tag '# A03'
    tableDF = pool.map(GDELTedaGKGhelpers.applyDtypes, [tableDF])
    pool.close()
    pool.join()
    print("    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))
    tableDF = tableDF.pop()

    timecheck = time()
    print("\n  Converting datetimes...")
    pool = multiprocessing.Pool(1)
    #   For convertDatetimes documentation, See GDELTedaGKGhelpers.py,
    # 'find' tag '# A04'
    tableDF = pool.map(GDELTedaGKGhelpers.convertDatetimes, [tableDF])
    pool.close()
    pool.join()
    print("    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))
    tableDF = tableDF.pop()
    
    timecheck = time()
    print("\n  Splitting V15Tone dicts to columns...")
    pool = multiprocessing.Pool(1)
    #   For convertGKGV15Tone code/documentation, See GDELTedaGKGhelpers.py,
    # 'find' tag '# A05'
    tableDF = pool.map(GDELTedaGKGhelpers.convertGKGV15Tone, [tableDF])
    pool.close()
    pool.join()
    print("    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))
    
    tableDF = tableDF.pop()
    print("\n  Main GKG records (non-variable-length columns) DataFrame",
          ".info():\n")
    pp(tableDF.info())

    #   Generating report excluding fields that require substantially more
    # records (normalization), and so more resources for reporting.
    timecheck = time()
    print("\n  Generating non-variable-length subfield report...")
    pool = multiprocessing.Pool(1)
    #   For mainReport code/documentation, See GDELTedaGKGhelpers.py,
    # 'find' tag '# A06'
    booleanSuccess = pool.map(GDELTedaGKGhelpers.mainReport, [tableDF])
    pool.close()
    pool.join()
    print("\n    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))

    # #   These columns may be dropped from this point on in order to
    # # accomodate the increased RAM, CPU, and disk I/O requirements for
    # # normalizing variable length columns, but this is commented out in order
    # # to further check RAM requirements for full normalization.
    # timecheck = time()
    # print("\n  Dropping excess columns before normalizing for variable-length",
    #       "columns...")
    # tableDF.drop(columns = ['V2SourceCommonName',
    #                         'V2DocumentIdentifier',
    #                         'V15Tone_Positive',
    #                         'V15Tone_Negative',
    #                         'V15Tone_Polarity',
    #                         'V15Tone_ARD',
    #                         'V15Tone_SGRD',
    #                         'V15Tone_WordCount'], inplace = True)
    # print("    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))

    # Working implementation with locationsReport
    timecheck = time()
    print("\n  Splitting V1Locations dicts and generating report...")
    pool = multiprocessing.Pool(1)
    #   For locationsReport code/documentation, See GDELTedaGKGhelpers.py,
    # 'find' tag '# A07'
    booleanSuccess = pool.map(GDELTedaGKGhelpers.locationsReport, [tableDF])
    pool.close()
    pool.join()
    print("    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))
    
    '''
    #   This section of calls are commented out due to their current inability
    # to complete processing for the batch EDA test subset of GKG records.
    #   Future attempts to complete these sections will see this comment area
    # removed and this section cleaned of work-in-progress documentation.
    
    #   Non-working implementation of countsReport. Tempted to think it's due
    # to Pandas limitations for long-running jobs, but it's also just a huge
    # DataFrame I'm attempting to normalize, thanks to the variable quantities
    # of 'V1Counts' values with subfielded values per record.
    # timecheck = time()
    # print("\n  Splitting V1Counts lists and generating report...")
    # pool = multiprocessing.Pool(1)
    # #   For countsReport code/documentation, See GDELTedaGKGhelpers.py,
    # # 'find' tag '# A08'
    # booleanSuccess = pool.map(GDELTedaGKGhelpers.countsReport, [tableDF])
    # pool.close()
    # pool.join()
    # print("    Complete! (%0.3f seconds)" % (float(time())-float(timecheck)))

    #   Ditto the rest of the normalization helper functions, because the
    # normalization necessary for EDA on this large a subset of GKG records is
    # just too high, given how this field has so many values of varying and
    # sometimes ridiculous length. If Pandas Profiling could be coerced to not
    # care about allocating smaller data structures for columns of varying
    # type, this wouldn't matter, because I could leave all but key column
    # values as NaN, but Pandas wants to allocate massive amounts of empty
    # memory structures, and when records total over 16 million and Pandas
    # wants that many 64-bit float values, even if there's only 8 million real
    # values in the column to work with, Pandas will create however many extra
    # copies of those 8 million empty 64-bit float memory signatures, or at
    # least demand them until the system can't help but except the process.


    timecheck = time()
    print("\n  Splitting V1Themes lists and generating report...")
    pool = multiprocessing.Pool(1)
    #   For themesReport code/documentation, See GDELTedaGKGhelpers.py,
    # 'find' tag '# A09'
    booleanSuccess = pool.map(GDELTedaGKGhelpers.themesReport, [tableDF])
    pool.close()
    pool.join()
    print("    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))

    timecheck = time()
    print("\n  Generating Persons report...")
    pool = multiprocessing.Pool(1)
    #   For personsReport code/documentation, See GDELTedaGKGhelpers.py,
    # 'find' tag '# A10'
    booleanSuccess = pool.map(GDELTedaGKGhelpers.personsReport, [tableDF])
    pool.close()
    pool.join()
    print("    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))

    timecheck = time()
    print("\n  Generating Organizations report...")
    pool = multiprocessing.Pool(1)
    #   For organizationsReport code/documentation, See GDELTedaGKGhelpers.py,
    # 'find' tag '# A11'
    booleanSuccess = pool.map(GDELTedaGKGhelpers.organizationsReport,[tableDF])
    pool.close()
    pool.join()
    print("    Complete! ( %0.3f s )" % (float(time())-float(timecheck)))
    
    '''

    print("All GKG EDA operations complete. Please check EDAlogs directories",
          " for any resulting EDA profile reports.")

    print("\n--------------------------------------------------------------\n")


  # B05
  def realtimeEDA(self, tableList = ['events','mentions','gkg']):
    '''Performs automatic EDA on the latest GDELT datafiles for records
 from Events/Mentions and GKG. This function is enabled by loopEDA() to
 download a specified window of datafiles, or else just most-recent
 datafiles if called by itself, or for a default loopEDA() call.

  Current testing on recent GDELT updates confirms that this function
may complete all EDA processing on each datafile set well within the
fifteen minute window before each successive update.

Parameters:
----------

tableList - list of strings, default ['events','mentions','gkg']
  Permits limiting of operations to one or more tables.

Output:
------

  Console displays progress through steps with time taken throughout,
and function generates EDA profile html documents in appropriate project
directories.
    '''
    if tableList != self.tableList:
      print("\n  Error: this GDELTeda object may have been initialized\n",
            "        without checking for the presence of directories\n",
            "        required for this function's operations.\n",
            "        Please check GDELTeda parameters and try again.")
      # using a tuple to track failed execution
      return (False, 'tableList')
   
    print("--------------------------------------------------------------\n")
    print("Beginning realtime GDELT EDA collection for these tables:")
    print(tableList)

    #   Decrementing remaining iterations to track 'last' run, in order to
    # delay report generation until desired window is collected.
    if self.realtimeWindow > 1:
      self.realtimeWindow -= 1
      self.realtimeLooping == True
    elif self.realtimeWindow == 1:
      self.realtimeWindow -= 1

    lastRun = False
    if self.realtimeWindow < 1:
      lastRun = True
    
    fileURLs = {
      'events'   : '',
      'mentions' : '',
      'gkg'      : '',
      }
    priorURLs = {
      'events'   : '',
      'mentions' : '',
      'gkg'      : '',
      }
    EDAFiles = {
      'events' : [],
      'mentions' : [],
      'gkg' : [],
      }

    # Tracking function runtime
    timecheckF = time()

    # applicable for all tables
    datetimeFormat = "%Y-%m-%dT%H:%M:%S.000000Z"
    strptimeFormat = "%Y%m%d%H%M%S"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    #   Downloading and parsing lastupdate.txt
    # That text file consists of three lines, one for each main GDELT
    # table's last datafile update. URLs/Filenames follow conventions used in
    # GDELTbase download, cleaning, and MongoDB export functions, e.g. a base
    # URL followed by 14 char numeric strings for the current UTC datetime at
    # the resolution of seconds, at 15-minute intervals, followed by csv and
    # zip extensions.
    #   As of 2021/09/08, that page is working and updates on the schedule
    # described in GDELT docs. Exact update times likely vary with volume of
    # current world news coverage, but should be at least every fifteen
    # minutes, with all dates and times reported by UTC zone.
    os.chdir(self.gBase.toolData['path']['base'])
    print("  Checking http://data.gdeltproject.org/gdeltv2/lastupdate.txt...")
    lastFilesURL = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    lastFiles = wget.download(lastFilesURL, 'lastupdate.txt')
    with open(lastFiles) as lastupdate:
      lines = lastupdate.readlines()

    #   Table order is reversed from most other loops in this project, here,
    # because list.pop() pulls in reverse and I'm too short on time to bother
    # juggling these strings more than necessary. Regardless, GKG is first in
    # this order, distinct from most elsewhere.
    for table in ['gkg', 'mentions', 'events']:
      fileURLs[table] = lines.pop().split(' ')[-1].replace("\n", '')

    # form a current datetime from lastupdate.txt strings
    thisDatetime = \
      datetime.strptime(fileURLs[table][37:51],
                        strptimeFormat).replace(tzinfo = timezone.utc)

    #   Form a 'last' datetime and string from the current lastupdates.txt
    # datetime string. Worst-case is dead midnight UTC ( 5pm PST, 6pm MST,
    # 8pm EST ) since the "last" file before then will be an irregular amount
    # of time prior: 00:00 UTC - 22:45 UTC = -1 hour 15 minutes
    if thisDatetime.hour == 0 and thisDatetime.minute == 0:
      lastDatetime = lastDatetime - timedelta(hours = 1)
    lastDatetime = (thisDatetime - timedelta(minutes = 15))
    lastDatestring = lastDatetime.strftime(strptimeFormat)

    #   First-run datetime, timedelta, and string juggling for generating
    # last-most-recent URLS for download.
    for table in ['gkg', 'mentions', 'events']:
      priorURLs[table] = ''.join([self.gBase.toolData['URLbase'],
                                  lastDatestring, '.',
                                  self.gBase.toolData['extensions'][table]])

    #   Shouldn't apply for first run, since no last/next file is set yet, and
    # shouldn't matter for the last run, since self.realtimeWindow running out
    # will halt execution in loopEDA() anyway.
    if self.lastRealDatetime != '' and self.nextRealDatetime != '':
      if thisDatetime == self.lastRealDatetime:
        print("\n----------------------------------------------------------\n")
        print("Isn't %s the same as %s ? Too early! No new update yet!" %
              (thisDatetime.strftime[strftimeFormat],
               self.lastRealDatetime.strftime[strftimeFormat]))
        return (False, 'tooEarly')
      elif thisDatetime > self.nextRealDatetime:
        print("\n----------------------------------------------------------\n")
        print("%s is a little later than %s . Too late! We missed one!" %
              (thisDatetime.strftime[strftimeFormat],
               self.lastRealDatetime.strftime[strftimeFormat]))
        return (False, 'tooLate')

    print("  URLs acquired:\n")
    print("current:")
    pp(fileURLs)
    print("prior:")
    pp(priorURLs)

    print("Beginning per-table operations...\n")

    for table in tableList:
      # B05a - every-table operations
      #   Note that order of execution for all tables will be project-typical.

      # Tracking per-table loop times
      timecheckT = time()
      print("Trying downloading and cleaning for most recent", table,
            "file...")
      # making use of alternate-mode functionality for GDELTbase methods.
      thisDL = self.gBase.downloadGDELTFile(fileURLs[table], table,
                                            verbose = True, mode = 'realtime')

      #   Matching the same input formatting requirements, typically performed
      # in the 'table' versions of GDELTbase methods
      fileName = fileURLs[table].replace(self.gBase.toolData['URLbase'], '')
      fileName = fileName.replace('.zip', '')
      # cleaning the file (exported to realtimeClean as .json)
      thisClean = self.gBase.cleanFile(fileName, verbose = True,
                                       mode = 'realtime')

      # tracking prior URLs, still, might delete this section
      lastFileName = priorURLs[table].replace(self.gBase.toolData['URLbase'], '')
      lastFileName = lastFileName.replace('.zip', '')


      # GKG still has different extensions...
      if table == 'gkg':
        cleanFileName = fileName.replace('.csv', '.json')
        cleanLastFileName = lastFileName.replace('.csv', '.json')
      else:
        cleanFileName = fileName.replace('.CSV', '.json')
        cleanLastFileName = lastFileName.replace('.CSV', '.json')
      
      
      #   Each iterative run of this function will add another most-recent
      # datafile, so long as it hasn't already been collected and cleaned, but
      # the first run should wipe per-table collections before populating 'em
      # with records.
      if not self.realtimeStarted:
        print("  Dropping any old realtime GDELT MongoDB collection...")
        self.gBase.localDb['collections']['realtime'][table].drop()
      print("Starting MongoDB export for acquired file...")
      thisMongo = self.gBase.mongoFile(cleanFileName, table, verbose = True,
                                  mode = 'realtime')
      print('')
      
      # Permitting delay of report generation for N iterations
      if lastRun:
        pass
      # bails on this loop iteration if not final realtimeEDA() iteration
      else:
        continue

      #   If lastRun == True, EDA processing will be executed in this iteration
      # for any records in the 'realtime' MongoDB collection for this table.

      print("Beginning EDA processing...")

      # switching to table-appropriate logPath directory...
      os.chdir(self.logPath[table]['realtime'])

      # B05b - Events/Mentions handling
      #   Per-table records querying, DataFrame shaping, and Pandas Profiling
      # EDA ProfileReport() generation.
      if table == 'events' or table == 'mentions':
        timecheckG = time()
        print("\n  Loading", table, "realtimeEDA files held locally...",
              end = '')
        thisDF = pd.DataFrame.from_records(list(
          self.gBase.localDb['collections']['realtime'][table].find(
            projection = {"_id" : 0},
            allow_disk_use = True,
            no_cursor_timeout = True,
            ),
          ), columns = self.gBase.toolData['names'][table]['reduced'])
        print("  ")
        print("  Setting dtypes...")
        thisDF = thisDF.astype(
          dtype = self.gBase.toolData['columnTypes'][table],
          copy = False,
          )
        print("  Converting datetimes...")
        if table == 'events':
          datetimeField = 'DATEADDED'
        # mentions has an extra datetime field, 'EventTimeDate', converted here
        if table == 'mentions':
          datetimeField = 'MentionTimeDate'
          thisDF['EventTimeDate'] = pd.to_datetime(thisDF['EventTimeDate'],
                                                   format = datetimeFormat)
        thisDF[datetimeField] = pd.to_datetime(thisDF[datetimeField],
                                               format = datetimeFormat)

        print("\n ", table, "DataFrame .info():\n")
        print(thisDF.info(),'\n')

        edaDateString = thisDF[datetimeField].min().strftime(strftimeFormat)
        if table == 'events':
          configName = "GDELTeventsEDAconfig_realtime.yaml"
          edaLogName = ''.join(["GDELT_Events_realtime_EDA_", edaDateString,
                                ".html"])
        if table == 'mentions':
          configName = "GDELTmentionsEDAconfig_realtime.yaml"
          edaLogName = ''.join(["GDELT_Mentions_realtime_EDA_", edaDateString,
                                ".html"])

        print("    File to output:", edaLogName)
        profile = ProfileReport(thisDF, config_file = configName)
        print("    Generating html from report...")
        profile.to_file(edaLogName)
        EDAFiles[table].append(edaLogName)
        del profile
        del thisDF
        print('')
        print('------------------------------------------------------------\n')

      # B05c - GKG handling
      if table == 'gkg':

        print("\n  Pulling any", table, "realtime EDA files...", end = '')
        timecheckG = time()
        thisDF = pd.DataFrame.from_records(list(
          self.gBase.localDb['collections']['realtime'][table].find(
            projection = {"_id" : 0},
            allow_disk_use = True,
            no_cursor_timeout = True,
            ),
          ), columns = self.gBase.toolData['names']['gkg']['reduced'])
        print(" ( %0.3f s )" % (float(time()) - float(timecheckG)))

        #   Reusing GDELTedaGKGhelpers.py functions, since they'll work
        # in this context. See that file for code and documentation.

        timecheckG = time()
        print("  Applying initial dtypes...", end = '')
        thisDF = GDELTedaGKGhelpers.applyDtypes(thisDF)
        print(" ( %0.3f s )" % (float(time()) - float(timecheckG)))
        
        timecheckG = time()
        print("  Converting datetimes...", end = '')
        thisDF = GDELTedaGKGhelpers.convertDatetimes(thisDF)
        print(" ( %0.3f s )" % (float(time()) - float(timecheckG)))
        
        edaDateString = thisDF['V21DATE'].min().strftime(strftimeFormat)
        
        timecheckG = time()
        print("  Splitting and forming columns from V15Tone...")
        thisDF = GDELTedaGKGhelpers.convertGKGV15Tone(thisDF)
        print(" ( took %0.3f s )" % (float(time()) - float(timecheckG)))

        # B05d - GKG non-variable-length EDA generation
        #   Isolating main columns for their own EDA, dropping variable-length
        # columns for copy (not inplace).
        timecheckG = time()
        print("  Starting EDA generation for main GKG columns only...", end='')
        mainDF = thisDF.drop(columns = ['V1Locations',
                                        'V1Counts',
                                        'V1Themes',
                                        'V1Persons',
                                        'V1Organizations'])
        print(" ( drop/copy: %0.3f s )" % (float(time()) - float(timecheckG)))

        print("\n  GKG main columns DataFrame .info():\n")
        print(mainDF.info())
        print('')

        # constructing EDA output filename
        configName = "GDELTgkgMainEDAconfig_realtime.yaml"
        edaLogName = ''.join(["GDELT_GKG_realtime_main_EDA_", edaDateString,
                             ".html"])

        # Generating non-variable-length-subfield column EDA
        print("\n  File to output:", edaLogName)
        profile = ProfileReport(mainDF, config_file = configName)        
        print("    Generating html from report...")
        profile.to_file(edaLogName)
        EDAFiles[table].append(edaLogName)
        print("\n ( ProfileReport() + .to_file() : %0.3f s )" %
              (float(time()) - float(timecheckG)))
        del profile
        del mainDF

        print("  Continuing processing with separate normalization of each",
              "variable-length subfield...\n")

        # B05e - V1Locations EDA generation
        timecheckG = time()
        print("  Exploding V1Locations...", end = '')
        locationDF = thisDF.drop(columns = ['V1Counts',
                                            'V1Themes',
                                            'V1Persons',
                                            'V1Organizations'])
        locationDF = locationDF.explode('V1Locations')
        print(" ( drop/explode: %0.3f s )" % \
              (float(time()) - float(timecheckG)))
        timecheckG = time()
        print("  Normalizing V1Locations...", end = '')
        subcols = pd.json_normalize(locationDF['V1Locations'])
        print(" ( %0.3f s )" % (float(time()) - float(timecheckG)))
        timecheckG = time()
        print("  Renaming columns, dropping old, rejoining, astyping...",
              end = '')
        subcols.columns = [f"V1Locations_{c}" for c in subcols.columns]
        locationDF = locationDF.drop(columns = ['V1Locations']).join(
          subcols).astype({'V1Locations_FullName'    : pd.StringDtype(),
                           'V1Locations_CountryCode' : pd.StringDtype(),
                           'V1Locations_ADM1Code'    : pd.StringDtype(),
                           'V1Locations_FeatureID'   : pd.StringDtype(),},
                          copy = False)
        print(" ( %0.3f s )" % (float(time()) - float(timecheckG)))

        print("\n  GKG Locations-normalized DataFrame .info():\n")
        print(locationDF.info())
        
        print("  Setting index to 'GKGRECORDID'...")
        locationDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                           inplace = True, verify_integrity = False)

        configName = "GDELTgkgLocationsEDAconfig_realtime.yaml"
        edaLogName = ''.join(["GDELT_GKG_realtime_locations_EDA_",
                              edaDateString,
                              ".html"])

        timecheckG = time()
        print("\n  File to output:", edaLogName)
        profile = ProfileReport(locationDF, config_file = configName)
        print("    Generating html from report...")
        profile.to_file(edaLogName)
        EDAFiles[table].append(edaLogName)
        print("\n ( ProfileReport() + .to_file() : %0.3f s )" %
              (float(time()) - float(timecheckG)))

        del locationDF
        del profile

        # B05f - V1Counts EDA generation
        timecheckG = time()
        print("  Exploding V1Counts...", end = '')
        countsDF = thisDF.drop(columns = ['V1Locations',
                                          'V1Themes',
                                          'V1Persons',
                                          'V1Organizations'])
        print(" ( drop/explode: %0.3f s )" % \
              (float(time()) - float(timecheckG)))
        countsDF = countsDF.explode('V1Counts')
        print("  Normalizing V1Counts...", end = '')
        subcols = pd.json_normalize(countsDF['V1Counts'])
        print(" ( %0.3f s )" % (float(time()) - float(timecheckG)))
        timecheckG = time()
        print("  Renaming columns, dropping old, rejoining,",
              "astyping... ", end = '')
        subcols.columns = [f"V1Counts_{c}" for c in subcols.columns]
        countsDF = countsDF.drop(columns = ['V1Counts']).join(
          subcols).astype({
            'V1Counts_CountType'           : pd.StringDtype(),
            'V1Counts_ObjectType'          : pd.StringDtype(),
            'V1Counts_LocationFullName'    : pd.StringDtype(),
            'V1Counts_LocationCountryCode' : pd.StringDtype(),
            'V1Counts_LocationADM1Code'    : pd.StringDtype(),
            'V1Counts_LocationFeatureID'   : pd.StringDtype(),
            }, copy = False)

        print("\n  GKG Counts-normalized DataFrame .info():\n")
        print(countsDF.info())

        print("  Setting index to 'GKGRECORDID'...")
        countsDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                           inplace = True, verify_integrity = False)

        configName = "GDELTgkgCountsEDAconfig_realtime.yaml"
        edaLogName = ''.join(["GDELT_GKG_realtime_counts_EDA_", edaDateString,
                             ".html"])
        timecheckG = time()
        print("\n  File to output:", edaLogName)
        profile = ProfileReport(countsDF, config_file = configName)
        print("\n    Generating html from report...")
        profile.to_file(edaLogName)
        EDAFiles[table].append(edaLogName)
        print("\n ( ProfileReport() + .to_file() : %0.3f s )" %
              (float(time()) - float(timecheckG)))
        del countsDF
        del profile

        # B05g - V1Themes EDA generation
        timecheckG = time()
        print("  Exploding V1Themes...", end = '')
        themesDF = thisDF.drop(columns = ['V1Locations',
                                          'V1Counts',
                                          'V1Persons',
                                          'V1Organizations'])
        themesDF = themesDF.explode('V1Themes')
        print(" ( drop/explode: %0.3f s )" %
              (float(time()) - float(timecheckG)))
        print("  Setting types...", end = '')
        themesDF = themesDF.astype({'V1Themes' : pd.StringDtype()},
                                   copy = False)

        print("\n  GKG Themes-normalized DataFrame .info():\n")
        print(themesDF.info())

        print("  Setting index to 'GKGRECORDID'...")
        themesDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                           inplace = True, verify_integrity = False)

        configName = "GDELTgkgThemesEDAconfig_realtime.yaml"
        edaLogName = ''.join(["GDELT_GKG_realtime_themes_EDA_", edaDateString,
                             ".html"])

        print("\n  File to output:", edaLogName)
        profile = ProfileReport(themesDF, config_file = configName)
        print("\n    Generating html from report...")
        profile.to_file(edaLogName)
        EDAFiles[table].append(edaLogName)
        print("\n ( ProfileReport() + .to_file() : %0.3f s )" %
              (float(time()) - float(timecheckG)))
        del themesDF
        del profile

        # B05h - V1Persons EDA generation
        timecheckG = time()
        print("  Exploding V1Persons...", end = '')
        personsDF = thisDF.drop(columns = ['V1Locations',
                                           'V1Themes',
                                           'V1Counts',
                                           'V1Organizations'])
        personsDF = personsDF.explode('V1Persons')
        print(" ( drop/explode: %0.3f s )" %
              (float(time()) - float(timecheckG)))
        print("  Setting types...", end = '')
        personsDF = personsDF.astype({'V1Persons' : pd.StringDtype()},
                                     copy = False)

        print("\n  GKG Persons-normalized DataFrame .info():\n")
        print(personsDF.info())

        print("  Setting index to 'GKGRECORDID'...")
        personsDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                            inplace = True, verify_integrity = False)

        configName = "GDELTgkgPersonsEDAconfig_realtime.yaml"
        edaLogName = ''.join(["GDELT_GKG_realtime_persons_EDA_", edaDateString,
                             ".html"])

        timecheckG = time()
        print("\n  File to output:", edaLogName)
        profile = ProfileReport(personsDF, config_file = configName)
        print("\n    Generating html from report...")
        profile.to_file(edaLogName)
        EDAFiles[table].append(edaLogName)
        print("\n ( ProfileReport() + .to_file() : %0.3f s )" %
              (float(time()) - float(timecheckG)))
        del personsDF
        del profile

        # B05i - V1Organizations EDA generation
        timecheckG = time()
        print("  Exploding V1Organizations...", end = '')
        orgDF = thisDF.drop(columns = ['V1Locations',
                                       'V1Themes',
                                       'V1Persons',
                                       'V1Counts'])
        orgDF = orgDF.explode('V1Organizations')
        print(" ( drop/explode: %0.3f s )" %
              (float(time()) - float(timecheckG)))
        timecheckG = time()
        print("  Setting types...", end = '')
        orgDF = orgDF.astype({'V1Organizations' : pd.StringDtype()},
                             copy = False)

        print("\n  GKG Organizations-normalized DataFrame .info():\n")
        print(orgDF.info())

        print("  Setting index to 'GKGRECORDID'...")
        orgDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                        inplace = True, verify_integrity = False)

        configName = "GDELTgkgOrganizationsEDAconfig_realtime.yaml"
        edaLogName = ''.join(["GDELT_GKG_realtime_organizations_EDA_",
                              edaDateString,
                              ".html"])

        timecheckG = time()
        print("\n  File to output:", edaLogName)
        profile = ProfileReport(orgDF, config_file = configName)
        print("\n    Generating html from report...")
        profile.to_file(edaLogName)
        EDAFiles[table].append(edaLogName)
        print("\n ( ProfileReport() + .to_file() : %0.3f s )" %
              (float(time()) - float(timecheckG)))
        del orgDF
        del profile
        del thisDF

      print(" Realtime", table,"EDA complete! ( total time taken: %0.3f s )"%
            (float(time())-float(timecheckT)))
      print("\n------------------------------------------------------------")
      # end of per-table loop
    
    # B05j - End-of-function/iteration calls

    # Continuing logic for delaying EDA generation
    if not lastRun:
      self.realtimeStarted = True
      print(" Delaying report generation until all updates are acquired...")
      print(" Updates to go:", self.realtimeWindow)
      self.realtimeLooping = True
      # maintaining cross-iteration tracking for comparison w/ lastupdate.txt and
      # maintaining datafile continuity.
      self.lastRealDatetime = thisDatetime
      self.nextRealDatetime = thisDatetime + timedelta(minutes = 15)
      inGap = False
      # gap in datafiles past 10:45pm UTC, until 12:00am UTC (1 hour 15 minutes)
      if self.nextRealDatetime.hour > 22:
        self.nextRealDatetime = thisDatetime + timedelta(hours = 1)
        inGap = True
      # check for 22:45 to 00:00 gap, controls 'wait' time in loopEDA()
      if inGap == True:
        return (True, 'inGap')
      else:
        return (True, 'continue')

    # Last run cleanup and returning confirmation to loopEDA()
    else:

      print("\n************************************************************\n")

      print("All table EDA reports generated! ( %0.2f s )\n" % \
            (float(time())-float(timecheckF)))
      print("EDA ProfileReport HTML files created:")
      pp(EDAFiles)
      print('')
      self.gBase.showLocalFiles(full = True, mode = 'realtime')
      self.realtimeLooping == False
      self.realtimeStarted == False
      self.realtimeWindow = 0
      self.lastRealDatetime = ''
      self.nextRealDatetime = ''
      return (True, 'finished')


  # B06
  def loopEDA(self, window = 1, windowUnit = 'file',
              tableList = ['events','mentions','gkg']):
    '''Loops calls of realtimeEDA() N iterations, where N is equal to
 'window' multiplied appropriately for windowUnit. Handles various
 potential failure states and delays execution of iterations until each
 next set of datafiles should be available.
 
   Note: this function has specifically been designed to collect current
 and future GDELT datafiles, rather than collecting specified windows
 within however many updates prior to the current update, as that
 functionality can already be achieved with GDELTbase.py or GDELTjob.py
 class member functions.

Parameters:
----------

window - int, default 1
  Controls the number of sets of datafile updates to download, dictated
 by the value set for 'windowUnit'. See that parameter's description for
 how 'window' relates to the number of iterations of realtimeEDA().

windowUnit - string, default 'file'
   May be one of 'day', 'hour', or 'file'. Controls the number of
 iterations realtimeEDA() will be looped.
   GDELT updates once every 15 minutes for Events/Mentions and GKG 2.0.
   This means a unit of 'day' will result in 93 files downloaded per int
 total in 'window', for example (be aware of the 1 hour, 15 minute gap
 after UTC 22:45), or 4 files per 'hour', or 1 file per 'file', but as
 a measure of overaccountability, this function will also download the
 last-most-recent file by default for any realtimeWindow value greater
 than 1, just to guarantee coverage of the window intended.

tableList - list of strings, default ['events','mentions','gkg']
  Permits limiting of operations to one or more tables.

Output:
------

  Console displays progress through steps with time taken throughout,
and function generates EDA profile html documents in appropriate project
directories.
  Each 'wait' period will display the time remaining before the next update
once every 60 seconds. In the case of UTC 22:45 to UTC 00:00, the function will
wait at most 1 hour and 15 minutes before checking for another set of updates.
    '''
    if windowUnit not in ['day', 'hour', 'file']:
      print("\n  Error: bad windowUnit value. Please specify one of 'day',\n",
            "        'hour', or 'file'")
      return False
    if window < 1:
      print("\n Error: bad window value. Please specify an integer > 0.")
      return False
    # looping behavior limited by default
    # Setting iterations count by unit and value
    if windowUnit == 'day':
      self.realtimeWindow = (window * 93)
    elif windowUnit == 'hour':
      self.realtimeWindow = (window * 4)
    elif windowUnit == 'file':
      self.realtimeWindow = window

    if tableList != self.tableList:
      print("\n  Error: this GDELTeda object may have been initialized\n",
            "        without checking for the presence of directories\n",
            "        required for this function's operations.\n",
            "        Please check GDELTeda parameters and try again.")
      return False
    
    print("Starting up realtimeEDA() looping...")
    print("  Window set by parameters:", window, "'%ss'" % windowUnit)
    print("  Tables to acquire/process/EDA:", tableList)
    print("  Sets of updates to acquire:", self.realtimeWindow)
    print('')

    # controlling for single-call default vs. longer window collection
    if self.realtimeWindow > 1:
      oneRun = False
      self.realtimeLooping = True
    else:
      oneRun = True

    # 'oneRun' iteration prior to loop, looping or not.
    timecheckG = time()
    self.realtimeEDA(tableList)
    timeTaken = float(time()) - float(timecheckG)

    # normal looping behavior mandates a pause, here
    if oneRun:
      print(" Single realtimeEDA() iteration complete.")
      print(" (%d seconds or %0.2f minutes))" %
            (timeTaken, float(timeTaken)/float(60)))
      return True

    # First wait period prior to loop, if looping.
    #   Check the current datetime against self.nextRealDatetime, then wait the
    # difference before running another realtimeEDA() iteration.
    timeNow = datetime.now(timezone.utc)
    timeLeft = (self.nextRealDatetime - timeNow).seconds
    if timeLeft > 0:
      print("Time remaining before next update at", self.nextRealDatetime,
            "\n  %d seconds (%0.2f minutes)" %
            (timeLeft, float(timeLeft)/float(60)))
      print("Current UTC time:", timeNow)
      print("Waiting", timeLeft, "seconds before attempting next iteration...")
      while(timeLeft > 60):
        sleep(60)
        timeLeft -= 60
        print("Waiting",timeLeft,"seconds before attempting next iteration...")
      sleep(timeLeft)
    else:
      print("  Warning: unexpected datetime issue.")
      print("  Waiting 15 minutes minus the time taken for first iteration",
            "before attempting next iteration.")
      timeLeft = 900 - timeTaken
      while(timeLeft > 60):
        timeLeft -= 60
        sleep(60)
        print("Waiting",timeLeft,"seconds before attempting next iteration...")
      sleep(timeLeft)

    timecheckG = time()
    # loops until failure or realtimeWindow iterations
    # 'firstRun' call above sets realtimeLooping to True
    # failure states or completion of all iterations sets it to false
    while self.realtimeLooping:
      #  tracking each iteration's runtime to completion as a failsafe
      timecheckL = time()
      #   realtimeEDA() call returns tuple of Boolean success/failure and
      # a string for that iteration's result.
      print("  Wait complete! Trying next iteration...")
      print("\n------------------------------------------------------------\n")
      thisEDA = self.realtimeEDA(tableList)
      lastTimeTaken = (float(time()) - float(timecheckL))

      # state handling for iteration results
      if thisEDA[0] == False:
        # failure states
        print("\nFailed iteration!")
        if thisEDA[1] == 'tableList':
          #   There's handling elsewhere for this, so this redundant state is
          # mostly for consistency.
          print("  Failed due to bad parameter 'tableList'.")
          return False
        if thisEDA[1] == 'tooEarly':
          #   Since datafile updates might be subject to unexpected delays,
          # this case handles them in the simplest possible way by forcing a
          # minute's delay before attempting another iteration for the same
          # files. Given the need for completing each update's processing
          # within 15 minutes, this short wait is intended for limiting delay.
          print("  Failed due to early execution (next datafile not found)")
          print("  Waiting 30 seconds before trying again...")
          sleep(30)
        if thisEDA[1] == 'tooLate':
          #   Since the goal is to collect each datafile reliably, future
          # handling of 'late' iteration attempts should fill in whatever gap
          # is left in the time available between future updates, if possible.
          #   For now, this is just a failure case, but future work can make
          # this case handleable.
          print("Couldn't complete processing before next datafile update!")
          print("Ending loopEDA(). Please check project directories for",
                "results from prior successful iterations, if any.")
          return False
      else:
        # success states, thisEDA[0] == True
        print("\nSuccessful iteration!")
        if thisEDA[1] == 'finished':
          # success and termination of loop
          print("  loopEDA() repetition of realtimeEDA finished successfully.")
          print("  Please check project directories for any generated files.",
                "\n    (see above console output for each iteration's",
                "displayed results)")
          break
        else:
          #   When loop isn't finished but running successfully, each iteration
          # needs to be delayed according to when the next update will be.
          timeNow = datetime.now(timezone.utc)
          timeLeft = (self.nextRealDatetime - timeNow).seconds
          # 'inGap' case, greater wait time required before next update
          if thisEDA[1] == 'inGap':
            #   After any call for 22:45 UTC files, will need to wait until
            # 00:00 for next files.
            if timeLeft > 0:
              print("\nExtra wait necessary due to gap before next",
                    "datafiles at", self.nextRealDatetime)
              print("Waiting", timeLeft, "seconds before attempting next",
                    "iteration... (e.g., %0.2f minutes, or %0.2f hours)" %
                    (float(timeLeft)/float(60),
                     float(timeLeft)/float(60)/float(60)))
              print("Current UTC time:", timeNow)
              print("Waiting", timeLeft, "seconds before attempting next",
                    "iteration...")
              while(timeLeft > 60):
                sleep(60)
                timeLeft -= 60
                print("Waiting",timeLeft,"seconds before attempting next",
                      "iteration...")
              sleep(timeLeft)
            else:
              print("  Warning: unexpected datetime issue.")
              print("  Waiting 15 minutes minus the time taken for first",
                    "iteration before attempting next iteration.")
              timeLeft = 900 - timeTaken
              while(timeLeft > 60):
                timeLeft -= 60
                sleep(60)
                print("Waiting",timeLeft,"seconds before attempting next",
                      "iteration...")
              sleep(timeLeft)
          #  'continue' case, wait the normal 15 minutes minus however long the
          # last iteration required
          if thisEDA[1] == 'continue':
            if timeLeft > 0:
              print("Time remaining before next update at",
                    self.nextRealDatetime, "\n  %d seconds (%0.2f minutes)" %
                    (timeLeft, float(timeLeft)/float(60)))
              print("Current UTC time:", timeNow)
              print("Waiting", timeLeft, "seconds before attempting next",
                    "iteration...")
              while(timeLeft > 60):
                sleep(60)
                timeLeft -= 60
                print("Waiting",timeLeft,"seconds before attempting next",
                      "iteration...")
              sleep(timeLeft)
            else:
              print("  Warning: unexpected datetime issue.")
              print("  Waiting 15 minutes minus the time taken for first",
                    "iteration",
                    "before attempting next iteration.")
              timeLeft = 900 - timeTaken
              while(timeLeft > 60):
                timeLeft -= 60
                sleep(60)
                print("Waiting",timeLeft,"seconds before attempting next",
                      "iteration...")
              sleep(timeLeft)
      # loop ends, function ends


# C00
# in-design iterative testing with direct execution
if __name__ == "__main__":
  # C01
  ''' This commented-out section is for saving previously-run test code, which
# may be of use in developing an understanding of this class and its methods,
# used here to perform testing on realtime EDA functionality.

  print("####################################################################")
  print("##########               GDELTeda testing                 ##########")
  print("####################################################################")

  print("\nPerforming testing for EDA on these tables, realtime records:")
  tables = ['events',
            'mentions',
            'gkg']
  print([i for i in tables])

  #   GDELTeda instance builds its own log directories one directory up from
  # the location of this script, in 'EDAlogs', with subdirectories for each
  # table and type of EDA ('batch' or 'realtime').
  gEDA01 = GDELTeda(tables)
  
  
  #   Each GDELTeda instance has its own instance of GDELTbase for convenience,
  # given end-user access to helper functions like this one.
  #   Since realtimeEDA() adds records to each table's realtime directories,
  # successive calls of realtimeEDA() or loopEDA() will result in those files
  # being preserved in table-appropriate 'realtimeRaw' and 'realtimeClean'
  # subdirectories in 'GDELTdata'. For testing, this call wipes those
  # 'realtime' data directories of raw and cleaned datafiles, at your
  # discretion.
  gEDA01.gBase.wipeLocalFiles(state = 'realtime', verbose = True)
  

  #   Default for realtimeEDA() parameters (no specification) is 1 file
  # downloaded per table, one profile produced per 'events' and 'mentions', and
  # six profiles produced for 'gkg' (main, locations, counts, themes, persons,
  # and organizations).
  gEDA01.realtimeEDA()
'''
