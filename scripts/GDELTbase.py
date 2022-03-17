'''
Project: WGU Data Management/Analytics Undergraduate Capstone
Richard Smith
August 2021

GDELTbase.py

  Class for creating/maintaining data directory structure, bulk downloading of
GDELT files with column reduction, parsing/cleaning to JSON format, and export
of cleaned records to MongoDB.

  Basic use should be by import and implementation within an IDE, or by editing
section # C00 and running this script directly.

  Primary member functions include descriptive docstrings for their intent and
use.

  See license.txt for information related to each open-source library used.

  WARNING: project file operations are based on relative pathing from the
'scripts' directory this Python script is located in, given the creation of
directories 'GDELTdata' and 'EDAlogs' parallel to 'scripts' upon first
GDELTbase and GDELTeda class initializations.
If those directories are not already present, a fallback method for
string-literal directory reorientation may be found in GDELTbase shared class
data at this tag: # A01a - backup path specification.
Any given user's project directory must be specified there.
See also GDELTeda.py, tag # A02b - Project directory path, as any given user's
project directory must be specified for that os.chdir() call, also.

Contents:
  A00 - GDELTbase
    A01 - shared class data (toolData, localDb)
      A01a - backup path specification
      Note: Specification at A01a should be changed to suit a user's desired
            directory structure, given their local filesystem.
    A02 - __init__ w/ instanced data (localFiles)
  B00 - class methods
    B01 - updateLocalFilesIndex
    B02 - clearLocalFilesIndex
    B03 - showLocalFiles
    B04 - wipeLocalFiles
    B05 - extensionToTableName
    B06 - isFileDownloaded
    B07 - downloadGDELTFile
    B08 - downloadGDELTDay
    B09 - cleanFile (includes the following field/subfield parser functions)
      B09a - themeSplitter
      B09b - locationsSplitter
      B09c - personsSplitter
      B09d - organizationsSplitter
      B09e - toneSplitter
      B09f - countSplitter
      B09g - One-liner date conversion function for post-read_csv use
      B09h - llConverter
    B10 - cleanTable
    B11 - mongoFile
    B12 - mongoTable
  C00 - main w/ testing
'''

import pandas as pd
import numpy as np
import os
import pymongo
import wget
import json
from time import time
from datetime import datetime, tzinfo
from zipfile import ZipFile as zf
from pprint import pprint as pp
from urllib.error import HTTPError

# A00
class GDELTbase:
  '''Base object for GDELT data acquisition, cleaning, and storage.

Shared class data:
-----------------
toolData - dict with these key - value pairs:
  URLbase - "http://data.gdeltproject.org/gdeltv2/"
  path - os.path path objects, 'raw' and 'clean', per-table
  names - lists of string column names, per-table, original and reduced
  extensions - dict mapping table names to file extensions, per-table
  columnTypes - dicts mapping table column names to appropriate types

localDb - dict with these key - value pairs:
client - pymongo.MongoClient()
database - pymongo.MongoClient().capstone
collections - dict mapping table names to suitable mongoDB collections

Instanced class data:
--------------------

localFiles - dict, per-table keys for lists of local 'raw' and 'clean'
filenames

Class methods:
-------------

updateLocalFilesIndex()
clearLocalFilesIndex()
showLocalFiles()
wipeLocalFiles()
extensionToTableName()
isFileDownloaded()
downloadGDELTFile()
downloadGDELTDay()
cleanFile()
cleanTable()
mongoFile()
mongoTable()
  '''

  # A01 - shared class data
  toolData = {}
  # A01a - backup path specification
  #   Failsafe path for local main project directory. Must be changed to suit
  # location of any given end-user's 'script' directory in case directory
  # 'GDELTdata' is not present one directory up.
  toolData['projectPath'] = 'C:\\Users\\urf\\Projects\\WGU capstone'
  #  Controls generation of datafile download URLs in downloadGDELTDay()/File()
  toolData['URLbase'] = "http://data.gdeltproject.org/gdeltv2/"
  #  Used in forming URLs for datafile download
  toolData['extensions'] = {
    'events'   : "export.CSV.zip",
    'gkg'      : "gkg.csv.zip",
    'mentions' : "mentions.CSV.zip",
    }

  #  These paths are set relative to the location of this script, one directory
  # up, in 'GDELTdata', parallel to the script directory.
  toolData['path'] = {}
  toolData['path']['base']= os.path.join(os.path.abspath(__file__),
                                         os.path.realpath('..'),
                                         'GDELTdata')
  toolData['path']['events'] = {
    'table': os.path.join(toolData['path']['base'], 'events'),
    'raw': os.path.join(toolData['path']['base'], 'events', 'raw'),
    'clean': os.path.join(toolData['path']['base'], 'events', 'clean'),
    'realtimeR' : os.path.join(toolData['path']['base'], 'events',
                               'realtimeRaw'),
    'realtimeC' : os.path.join(toolData['path']['base'], 'events',
                               'realtimeClean')
    }
  toolData['path']['gkg'] = {
    'table': os.path.join(toolData['path']['base'], 'gkg'),
    'raw': os.path.join(toolData['path']['base'], 'gkg', 'raw'),
    'clean': os.path.join(toolData['path']['base'], 'gkg', 'clean'),
    'realtimeR' : os.path.join(toolData['path']['base'], 'gkg',
                               'realtimeRaw'),
    'realtimeC' : os.path.join(toolData['path']['base'], 'gkg',
                               'realtimeClean')
    }
  toolData['path']['mentions'] = {
    'table': os.path.join(toolData['path']['base'], 'mentions'),
    'raw': os.path.join(toolData['path']['base'], 'mentions', 'raw'),
    'clean': os.path.join(toolData['path']['base'], 'mentions', 'clean'),
    'realtimeR' : os.path.join(toolData['path']['base'], 'mentions',
                               'realtimeRaw'),
    'realtimeC' : os.path.join(toolData['path']['base'], 'mentions',
                               'realtimeClean')
    }
  
  #   These mappings and lists are for recognition of all possible
  # column names, and the specific discarding of a number of columns
  # which have been predetermined as unnecessary in the context of
  # simple EDA.
  toolData['names'] = {}
  toolData['names']['events'] = {
    'original' : [
      'GLOBALEVENTID',
      'Day',
      'MonthYear',
      'Year',
      'FractionDate',
      'Actor1Code',
      'Actor1Name',
      'Actor1CountryCode',
      'Actor1KnownGroupCode',
      'Actor1EthnicCode',
      'Actor1Religion1Code',
      'Actor1Religion2Code',
      'Actor1Type1Code',
      'Actor1Type2Code',
      'Actor1Type3Code',
      'Actor2Code',
      'Actor2Name',
      'Actor2CountryCode',
      'Actor2KnownGroupCode',
      'Actor2EthnicCode',
      'Actor2Religion1Code',
      'Actor2Religion2Code',
      'Actor2Type1Code',
      'Actor2Type2Code',
      'Actor2Type3Code',
      'IsRootEvent',
      'EventCode',
      'EventBaseCode',
      'EventRootCode',
      'QuadClass',
      'GoldsteinScale',
      'NumMentions',
      'NumSources',
      'NumArticles',
      'AvgTone',
      'Actor1Geo_Type',
      'Actor1Geo_FullName',
      'Actor1Geo_CountryCode',
      'Actor1Geo_ADM1Code',
      'Actor1Geo_ADM2Code',
      'Actor1Geo_Lat',
      'Actor1Geo_Long',
      'Actor1Geo_FeatureID',
      'Actor2Geo_Type',
      'Actor2Geo_FullName',
      'Actor2Geo_CountryCode',
      'Actor2Geo_ADM1Code',
      'Actor2Geo_ADM2Code',
      'Actor2Geo_Lat',
      'Actor2Geo_Long',
      'Actor2Geo_FeatureID',
      'ActionGeo_Type',
      'ActionGeo_FullName',
      'ActionGeo_CountryCode',
      'ActionGeo_ADM1Code',
      'ActionGeo_ADM2Code',
      'ActionGeo_Lat',
      'ActionGeo_Long',
      'ActionGeo_FeatureID',
      'DATEADDED',
      'SOURCEURL',
      ],
    'reduced' : [
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
      ],
    }
  toolData['names']['gkg']  = {
    'original' : [
      'GKGRECORDID',
      'V21DATE',
      'V2SourceCollectionIdentifier',
      'V2SourceCommonName',
      'V2DocumentIdentifier',
      'V1Counts',
      'V21Counts',
      'V1Themes',
      'V2EnhancedThemes',
      'V1Locations',
      'V2EnhancedLocations',
      'V1Persons',
      'V2EnhancedPersons',
      'V1Organizations',
      'V2EnhancedOrganizations',
      'V15Tone',
      'V21EnhancedDates',
      'V2GCAM',
      'V21SharingImage',
      'V21RelatedImages',
      'V21SocialImageEmbeds',
      'V21SocialVideoEmbeds',
      'V21Quotations',
      'V21AllNames',
      'V21Amounts',
      'V21TranslationInfo',
      'V2ExtrasXML',
      ],
    'reduced' : [
      'GKGRECORDID',
      'V21DATE',
      'V2SourceCommonName',
      'V2DocumentIdentifier',
      'V1Counts',
      'V1Themes',
      'V1Locations',
      'V1Persons',
      'V1Organizations',
      'V15Tone',
      ],
    }
  toolData['names']['mentions'] = {
    'original' : [
      'GLOBALEVENTID',
      'EventTimeDate',
      'MentionTimeDate',
      'MentionType',
      'MentionSourceName',
      'MentionIdentifier',
      'SentenceID', #
      'Actor1CharOffset',#
      'Actor2CharOffset',#
      'ActionCharOffset',#
      'InRawText',
      'Confidence',
      'MentionDocLen', #
      'MentionDocTone',
      'MentionDocTranslationInfo', #
      'Extras', #
      ],
    'reduced' : [
      'GLOBALEVENTID',
      'EventTimeDate',
      'MentionTimeDate',
      'MentionType',
      'MentionSourceName',
      'MentionIdentifier',
      'InRawText',
      'Confidence',
      'MentionDocTone',
      ],
    }
  
  #   These mappings are used in automated dtype application to Pandas
  # DataFrame collections of GDELT records, part of preprocessing.
  toolData['columnTypes'] = {}
  toolData['columnTypes']['events'] = {
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
  toolData['columnTypes']['gkg'] = {
    'GKGRECORDID' : pd.StringDtype(),
    'V21DATE' : pd.StringDtype(),
    'V2SourceCommonName' : pd.StringDtype(),
    'V2DocumentIdentifier' : pd.StringDtype(),
    }
  toolData['columnTypes']['mentions'] = {
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

  #   The pymongo MongoClient() specification here may be modified to
  # meet user requirements, given their own preference for non-default
  # specification of MongoDB instance connection parameters.
  localDb = {}
  localDb['client'] = pymongo.MongoClient()
  localDb['database'] = localDb['client'].capstone
  localDb['collections'] = {
    'events'   : localDb['database'].GDELT.events,
    'gkg'      : localDb['database'].GDELT.gkg,
    'mentions' : localDb['database'].GDELT.mentions,
    }
  localDb['collections']['realtime'] = {
    'events'   : localDb['database'].GDELT.realtime.events,
    'gkg'      : localDb['database'].GDELT.realtime.gkg,
    'mentions' : localDb['database'].GDELT.realtime.mentions,
    }


  # A02 - init w/ instanced local file detection
  def __init__(self):
    '''Initializes GDELTbase object with shared toolData and instanced
localFiles, checks for presence of base and table data directories and
creates them if not present, then runs updateLocalFilesIndex() to
populate localFiles.
    '''

    self.localFiles = {
      'events' : {
        'raw' : [],
        'clean' : [],
        'realtimeR' : [],
        'realtimeC' : [],
        },
      'gkg' : {
        'raw' : [],
        'clean' : [],
        'realtimeR' : [],
        'realtimeC' : [],
        },
      'mentions' : {
        'raw' : [],
        'clean' : [],
        'realtimeR' : [],
        'realtimeC' : [],
        },
      }

    # checking base data directory, create if not present
    if not os.path.isdir(self.toolData['path']['base']):
      # Makes use of backup path for project directory, string-literal
      os.chdir(self.toolData['projectPath'])
      os.mkdir(self.toolData['path']['base'])
    # switch to data directory
    os.chdir(self.toolData['path']['base'])

    for table in ['events', 'gkg', 'mentions']:
      thisTablePath = self.toolData['path'][table]['table']
      if os.path.isdir(thisTablePath):
        os.chdir(thisTablePath)
      else:
        os.mkdir(thisTablePath)
        os.chdir(thisTablePath)
      for state in ['raw', 'clean', 'realtimeR', 'realtimeC']:
        thisStatePath = self.toolData['path'][table][state]
        if not os.path.isdir(thisStatePath):
          os.mkdir(thisStatePath)
        os.chdir(thisTablePath)
      os.chdir(self.toolData['path']['base'])

    self.updateLocalFilesIndex()


# B00 - class methods

  # B01
  def updateLocalFilesIndex(self, verbose = False):
    '''Updates downloaded files lists with local GDELTdata files. Used
in class initialization.

Parameters:
----------

verbose - boolean, default False
  Controls printed output, default no output. If true, prints only
notification of method execution.

output:
------

  See parameter description for 'verbose' for printed output.
  Updates instanced data 'localFiles' dictionary of datafile lists.
    '''
    self.clearLocalFilesIndex()
    if verbose:
      print("  Updating local files index...")
    os.chdir(self.toolData['path']['base'])
    for table in ['events', 'gkg', 'mentions']:
      thisTablePath = self.toolData['path'][table]['table']
      os.chdir(thisTablePath)
      for state in ['raw', 'clean', 'realtimeR', 'realtimeC']:
        os.chdir(self.toolData['path'][table][state])
        for fileName in os.listdir(self.toolData['path'][table][state]):
          if fileName not in self.localFiles[table][state]:
            self.localFiles[table][state].append(fileName)
      os.chdir(self.toolData['path']['base'])

  # B02
  def clearLocalFilesIndex(self):
    '''Used by updateLocalFilesIndex(), wipes this class instance's
lists of local files.
    '''

    for table in ['events', 'gkg', 'mentions']:
      for state in ['raw', 'clean', 'realtimeR', 'realtimeC']:
        self.localFiles[table][state] = []


  # B03
  def showLocalFiles(self, full = False, mode = 'batch'):
    '''For displaying local files, either counts per-table or a full
list.

Parameters:
----------

full - boolean
  Default False, only show file counts. When True, displays full file
list with output.

mode - string, default 'batch'
  Controls function behavior, default is for calls by GDELTbase or
 GDELTjobs, reporting on batch datafile locations.
 for mass-download of GDELT datafiles. String 'realtime' is passed for
 calls in GDELTeda.realtimeEDA(), in which individual files are
 downloaded to separate directories and profiled individually.

output:
------

See parameter description for 'full' for printed output.
    '''
    self.updateLocalFilesIndex()
    if mode == 'batch':
      print("Files currently present in GDELTdata:")
    elif mode == 'realtime':
      print("Files currently present in GDELTdata realtime directories:")
    for table in ['events', 'gkg', 'mentions']:
      if mode == 'batch':
        print("Table: '%s', path: '%s'" % \
              (table, self.toolData['path'][table]['table']))
        for state in ['raw', 'clean', 'realtimeR', 'realtimeC']:
          print("  state: '%s', path: '%s'" % \
                (state, self.toolData['path'][table][state]))
          print("  %i files." % (len(self.localFiles[table][state])))
          if full:
            for file in self.localFiles[table][state]:
              print("    ", file)
      if mode == 'realtime':
        print("Table: '%s', path: '%s'" % \
              (table, self.toolData['path'][table]['table']))
        for state in ['realtimeR', 'realtimeC']:
          print("  state: '%s', path: '%s'" % \
                (state, self.toolData['path'][table][state]))
          print("  %i files." % (len(self.localFiles[table][state])))
          if full:
            for file in self.localFiles[table][state]:
              print("    ", file)
    print("")


  # B04
  def wipeLocalFiles(self, tableList = ['events','gkg','mentions'],
                     state = 'clean', verbose = False):
    '''QOL function for deleting downloaded GDELT files. Intended for
 use as a utility while testing other functions, following full data
 export to mongoDB in order to conserve disk space, and in GDELTeda for
 clearing raw and clean realtime datafiles (but not .../EDALogs/...
 files, for example.).

Parameters:
----------

tableList - list of strings, default ['events','gkg','mentions'].
  Must contain at least one value from among the above default values.
Controls what table's files will be deleted.

stateList - string, default 'clean'
   Controls what type of file will be deleted.
 These values are permitted:
   'clean' - default, removes cleaned GDELT json documents.
   'raw' - removes downloaded and extracted GDELT csv datafiles.
   'both' - both of the above operations.
   'realtime' - 'both', but for realtime datafile directories
   'all' - removes all datafiles from project-controlled directories.

verbose - boolean, default False
  Controls printed output, default 'terse' (prints counts for each
table/type). If true, prints entire list of all deleted files, organized
by table and type.

output:
-------

See parameter description for 'verbose' for printed output.

Deletes parameter-specified local GDELT files.
    '''

    stateDict = {
      'raw': ['raw'],
      'clean': ['clean'],
      'both': ['raw', 'clean'],
      'realtime': ['realtimeR', 'realtimeC'],
      'all' : ['raw', 'clean', 'realtimeR', 'realtimeC'],
    }
    stateList = stateDict[state]
    timecheck = time()
    filesWiped = 0
    os.chdir(self.toolData['path']['base'])
    print("Deleting files currently present in GDELTdata:")
    for table in tableList:
      os.chdir(self.toolData['path'][table]['table'])
      print("  Table:", table)
      for state in stateList:
        os.chdir(self.toolData['path'][table][state])
        print("    state:", state, ",",
              len(self.localFiles[table][state]), "files.")
        for fileName in os.listdir(self.toolData['path'][table][state]):
          filePath = os.path.join(self.toolData['path'][table][state],
                                  fileName)
          if verbose:
            print("      deleting", fileName, end = '...')
          if not os.path.isfile(filePath):
            print("  filePath failure!", filePath)
            continue
          else:
            try:
              os.remove(filePath)
              if verbose:
                print(" good delete!")
              self.localFiles[table][state].remove(fileName)
            except OSError as e:
              print("Error: %s : %s" % (filePath, e.strerror))
    print("  Finished in %d seconds\n" % (time() - timecheck))


  # B05
  def extensionToTableName(self, fileName):
    '''For use in isFileDownloaded() (used by downloadGDELTFile()) and
cleanTable.

Parameters:
----------

fileName - string
  string[15:] must be one of "export.CSV", "gkg.csv", or "mentions.CSV".

output:
------

returns extToTable[extension] - string,
one of ["events", "gkg", "mentions"]
    '''

    extension = fileName[15:]
    if extension not in ["export.CSV", "gkg.csv", "mentions.CSV"]:
      print("extensionToTableName error! fileName '%s'" % (fileName), end='')
      print(" produced extension '%s'" % (extension))
    extToTable = {
      'export.CSV' : 'events',
      'gkg.csv' : 'gkg',
      'mentions.CSV' : 'mentions',
    }
    return extToTable[extension]


  # B06
  def isFileDownloaded(self, fileName):
    '''Helper function, checks for filename presence in appropriate
local directories.

Parameters:
----------

fileName - string
  format: YYYYMMDDHHMMSS.[flag].[csv/CSV].zip, Used to interpret table
type and check appropriate file lists.

output:
------

boolean for fileName's presence in local raw or clean files, intended
for handling in downloadGDELTFile.
    '''

    table = self.extensionToTableName(fileName)
    return (fileName in self.localFiles[table]['raw']) \
      or (fileName in self.localFiles[table]['clean']) \
      or (fileName in self.localFiles[table]['realtimeR']) \
      or (fileName in self.localFiles[table]['realtimeC'])


  # B07
  def downloadGDELTFile(self, thisUrl, table, verbose = False, mode = 'batch'):
    '''Downloads and extracts *.csv.zip files to local GDELTdata dirs.

Parameters:
----------

thisUrl - string
  Typically formed by downloadGDELTDay() from url components:
<URLbase> + YYYYMMDDHHMMSS.[flag].[csv/CSV].zip

table - string
  Must be one of ['events', 'gkg', 'mentions'], controls what table's
file will be downloaded.

verbose - boolean, default False
  Controls printed output, default 'terse' (no output in this method,
downloadGDELTDay will still print counts).
If true, prints fileName and time to download.

mode - string, default 'batch'
  Controls function behavior, default is for calls by downloadGDELTDay()
 for mass-download of GDELT datafiles. String 'realtime' is passed for
 calls in GDELTeda.realtimeEDA(), in which individual files are
 downloaded to separate directories and profiled individually.

output:
------

See parameter description for 'verbose' for printed output.

Returns boolean handled by GDELTbase.downloadGDELTDay() and/or
GDELTeda.realtimeEDA(), for tracking results of operations.
    '''

    os.chdir(self.toolData['path'][table]['table'])
    fileName = thisUrl.replace(self.toolData['URLbase'],'')
    fileName = fileName.replace('.zip', '')
    if self.isFileDownloaded(fileName):
      if verbose:
        print(" ", fileName, "already downloaded, skipping...")
      return False
    else:
      if verbose:
        print("  Getting %s..." % (fileName), end='')

      # branching for GDELTeda.realtimeEDA() calls  
      if mode == 'batch':
        statePath = self.toolData['path'][table]['raw']
      elif mode == 'realtime':
        statePath = self.toolData['path'][table]['realtimeR']

      os.chdir(statePath)
      zipFileName = ''.join([fileName, '.zip'])
      filePath = os.path.join(statePath, zipFileName)
      timecheck = time()
      try:
        wget.download(thisUrl, filePath)
      except HTTPError:
        print("Error 404: File not found! File: %s" % (fileName))
        return False
      if verbose:
        print("done(%0.3fs)" % (float(time())-float(timecheck)), end=', ')
        print("extracting...", end=' ')
      with zf(filePath, 'r') as zipFile:
        zipFile.extractall()
      if verbose:
        print("done, ", end='')
      try:
        if verbose:
          print("removing zip file...", end='')
        os.remove(filePath)
      except OSError as e:
        print("Error: %s : %s" % (fileName, e.strerror))
      if verbose:
        print(" done.")
      fileName = fileName.replace('.zip','')
      if mode == 'batch':
        self.localFiles[table]['raw'].append(fileName)
      elif mode == 'realtime':
        self.localFiles[table]['realtimeR'].append(fileName)
      return True


  # B08
  def downloadGDELTDay(self, date, table, verbose = False):
    '''Downloads up to 93 15-minute window files for a given day and
table, calling downloadGDELTFile() for each file.

Parameters:
----------

date - string
  format 'YYYY/MM/DD', don't forget a leading '0' for months prior to
october (01 to 09)

table - string
  Must be one of ['events', 'gkg', 'mentions'], controls what table's
file will be downloaded.

verbose - boolean, default False
  Controls printed output, default 'terse' (only print basic current
state, counts for downloaded files, and time to completion).
If true, prints fileName and time to download for each individual file,
as well as a total count.

output:
------

Downloads raw GDELT files to local directories.

See parameter description for 'verbose' for printed output.

completion, default, or all URLs/filenames and times to completion.
    '''

    if table not in ['events', 'gkg', 'mentions']:
      print("Bad 'table' value, specify one of 'events', 'gkg', 'mentions'.")
    else:
      timecheck = time()
      filesDownloaded = []
      filesSkipped = []
      filePath = self.toolData['path'][table]['raw']
      os.chdir(filePath)
      print("Starting downloading and extraction for %s, table '%s'..." % \
            (date, table))
      date = date.replace("/", '')
      urlStart = ''.join([self.toolData['URLbase'], date])
      for hour in range(0, 23):
        if hour < 10:
          hour = '0' + str(hour)
        for minute in ['00', '15', '30', '45']:
          thisUrl = ''.join([urlStart, str(hour), str(minute), "00.",
                             self.toolData['extensions'][table]])
          downloaded = self.downloadGDELTFile(thisUrl, table, verbose)
          thisUrl = thisUrl.replace(self.toolData['URLbase'], '')
          thisUrl = thisUrl.replace('.zip', '')
          if downloaded:
            filesDownloaded.append(thisUrl)
          else:
            filesSkipped.append(thisUrl)
      print("Downloading and extraction complete (%0.3fs)." % \
            (float(time())-float(timecheck)))
      print("%d files acquired, %d files skipped" % \
            (len(filesDownloaded), len(filesSkipped)))


  # B09
  def cleanFile(self, fileName, deleteRaw = False, verbose = False,
                mode = 'batch'):
    '''Imports .CSV to Pandas dataframe while dropping unused columns,
corrects field-type mapping, parses fields when necessary, then exports
to file as JSON.

Parameters:
----------

fileName - string
  should match formatting for entries in localFiles[<table>]['raw']

deleteRaw - boolean, default False
  When True, the raw file will be deleted.

verbose - boolean, default False
  Controls printed output, default 'terse' (no output in this method,
cleanTable will still print counts).
If true, prints fileName conversions applied.

mode - string, default 'batch'
  Controls function behavior, default is for calls by
 GDELTbase.cleanTable() for mass-cleaning of GDELT datafiles. String
 'realtime' is passed for calls in GDELTeda.realtimeEDA(), in which
 individual files are downloaded and cleaned in separate directories and
 profiled individually.

output:
------

Stores reduced-size, parsed/cleaned-contents file copy in 'clean' dir,
appropriate to the table of the input file.

See parameter description for 'verbose' for printed output.

returns 'cleaned' - boolean, intended for success/fail reporting in
cleanTable()
    '''

    table = self.extensionToTableName(fileName)
    rawFileName = fileName
    if mode == 'batch':
      rawFilePath = os.path.join(self.toolData['path'][table]['raw'],
                                 rawFileName)
    elif mode == 'realtime':
      rawFilePath = os.path.join(self.toolData['path'][table]['realtimeR'],
                                 rawFileName)

    if table == 'gkg':
      cleanFileName = rawFileName.replace('.csv', '.json')
    else:
      cleanFileName = rawFileName.replace('.CSV', '.json')

    if verbose:
      print("  %s : " % (rawFileName), end='')

    # Branching for GDELTeda.realtimeEDA() use
    if mode == 'batch':
      if (cleanFileName in self.localFiles[table]['clean']):
        if verbose:
          print("already cleaned, skipping.")
        return False
      else:
        os.chdir(self.toolData['path'][table]['raw'])
        if not os.path.isfile(rawFilePath):
          if verbose:
            print("not found, skipping.")
          return False
    if mode == 'realtime':
      if (cleanFileName in self.localFiles[table]['realtimeC']):
        if verbose:
          print("already cleaned, skipping.")
        return False
      else:
        os.chdir(self.toolData['path'][table]['realtimeR'])
        if not os.path.isfile(rawFilePath):
          if verbose:
            print("not found, skipping.")
          return False

    if verbose:
      print("found, ", end='')

    if table == 'gkg':
      # GKG's subfield handling:
      #     Taking advantage of read_csv param 'converters' to save a
      # lot of individual *.apply() calls.
      #     (GKG record fields with subfield mappings require more
      # in-depth handling, due to varying delineation among the fields.
      # Just like *apply() routes conversion functions through Numpy's
      # 'C' backend for improved processing speed, 'converters'-passed
      # functions also benefit from Numpy's processing help).

      # B09a
      def themeSplitter(x):
        '''conversion function for parsing themes subfields
        '''
        if not pd.isnull(x):
          y = x.split(sep = ";")
          if y[-1] == '':
            y.remove('')
        else:
          y = [' ']
        return y

      # B09b
      def locationsSplitter(x):
        '''conversion function for parsing locations subfields
        '''
        if not pd.isnull(x):
          y = x.split(sep=";")
          locationDictTemplate = {
            'Type': None,
            'FullName': None,
            'CountryCode': None,
            'ADM1Code': None,
            'Latitude': None,
            'Longitude': None,
            'FeatureID': None,
          }
          locations = []
          for substring in y:
            locationDict = locationDictTemplate.copy()
            subloc = substring.split(sep = "#")
            if subloc[-1] == '':
              subloc.remove('')
            subLength = len(subloc)
            if subLength > 0:
              if (subloc[0] != ''):
                locationDict['Type'] = int(subloc[0])
            if subLength > 1:
              if (subloc[1] != ''):
                locationDict['FullName'] = subloc[1]
            if subLength > 2:
              if (subloc[2] != ''):
                locationDict['CountryCode'] = subloc[2]
            if subLength > 3:
              if (subloc[3] != ''):
                locationDict['ADM1Code'] = subloc[3]
            if subLength > 4:
              if (subloc[4] != ''):
                locationDict['Latitude'] = float(subloc[4])
            if subLength > 5:
              if (subloc[5] != ''):
                locationDict['Longitude'] = float(subloc[5])
            if subLength > 6:
              if (subloc[6] != ''):
                locationDict['FeatureID'] = subloc[6]
            locations.append(locationDict.copy())
        else:
          locations = None
        return locations

      # B09c
      def personsSplitter(x):
        '''conversion function for parsing persons subfields
        '''
        if not pd.isnull(x):
          y = x.split(sep=";")
          if len(y) > 1 and y[-1] == '':
            y.remove('')
        else:
          y = [' ']
        return y

      # B09d
      def organizationsSplitter(x):
        '''conversion function for parsing organizations subfields
        '''
        if not pd.isnull(x):
          y = x.split(sep=";")
        else:
          y = [' ']
        return y

      # B09e
      def toneSplitter(x):
        '''conversion function for parsing tone subfields
        '''
        if not pd.isnull(x):
          x = x.split(sep=",")
          toneDict = {
            'Tone': None,
            'Positive': None,
            'Negative': None,
            'Polarity': None,
            'ARD': None,
            'SGRD': None,
            'WordCount': None,
            }
          if len(x) > 6:
            if x[0] != '':
              toneDict['Tone'] = float(x[0])
            if x[1] != '':
              toneDict['Positive'] = float(x[1])
            if x[2] != '':
              toneDict['Negative'] = float(x[2])
            if x[3] != '':
              toneDict['Polarity'] = float(x[3])
            if x[4] != '':
              toneDict['ARD'] = float(x[4])
            if x[5] != '':
              toneDict['SGRD'] = float(x[5])
            if x[6] != '':
              toneDict['WordCount'] = int(x[6])
          return toneDict

      # B09f
      def countSplitter(x):
        '''conversion function for parsing count subfields
        '''
        if not pd.isnull(x):
          y = x.split(sep=";")
          if len(y) == 0:
            return None
          countDictTemplate = {
            'CountType' : None,
            'Count' : None,
            'ObjectType' : None,
            'LocationType' : None,
            'LocationFullName': None,
            'LocationCountryCode': None,
            'LocationADM1Code': None,
            'LocationLatitude': None,
            'LocationLongitude': None,
            'LocationFeatureID': None,
            }
          counts = []
          for substring in y:
            countDict = countDictTemplate.copy()
            subCount = substring.split(sep = "#")
            if subCount[-1] == '':
              subCount.remove('')
            subLength = len(subCount)
            # if subLength < 1:
            #   continue
            if subLength > 0:
              if (subCount[0] != ''):
                countDict['CountType'] = subCount[0]
            if subLength > 1:
              if (subCount[1] != ''):
                countDict['Count'] = int(subCount[1])
            if subLength > 2:
              if (subCount[2] != ''):
                countDict['ObjectType'] = subCount[2]
            if subLength > 3:
              if (subCount[3] != ''):
                countDict['LocationType'] = int(subCount[3])
            if subLength > 4:
              if (subCount[4] != ''):
                countDict['LocationFullName'] = subCount[4]
            if subLength > 5:
              if (subCount[5] != ''):
                countDict['LocationCountryCode'] = subCount[5]
            if subLength > 6:
              if (subCount[6] != ''):
                countDict['LocationADM1Code'] = subCount[6]
            if subLength > 7:
              if (subCount[7] != ''):
                countDict['LocationLatitude'] = float(subCount[7])
            if subLength > 8:
              if (subCount[8] != ''):
                countDict['LocationLongitude'] = float(subCount[8])
            if subLength > 9:
              if (subCount[9] != ''):
                countDict['LocationFeatureID'] = subCount[9]
            counts.append(countDict.copy())
        else:
          counts = None
        return counts

      # mapping conversion functions for read_csv param 'converters'
      gkgConverters = {
        'V1Themes' : themeSplitter,
        'V1Locations' : locationsSplitter,
        'V1Persons' : personsSplitter,
        'V1Organizations' : organizationsSplitter,
        'V15Tone' : toneSplitter,
        'V1Counts' : countSplitter,
        }
      cleanDF = pd.read_csv(rawFilePath, sep='\t',
                           names = self.toolData['names'][table]['original'],
                           usecols = self.toolData['names'][table]['reduced'],
                           dtype = self.toolData['columnTypes'][table],
                           converters = gkgConverters)
      if verbose:
        print("applying: The Loc Per Org Ton Co ", end='')
    else:
      # events and mentions files handled here
      cleanDF = pd.read_csv(rawFilePath, sep='\t',
                           names = self.toolData['names'][table]['original'],
                           usecols = self.toolData['names'][table]['reduced'],
                           dtype = self.toolData['columnTypes'][table])
      if verbose:
        print("applying: ", end='')
    
    # B09g - One-liner date conversion function for post-read_csv use
    dtConverter = lambda x: datetime.strptime(str(x), "%Y%m%d%H%M%S")

    # After catching strange occurrences of '#' in 'events' field values for
    #  'Actor1/2Geo_Lat/Long', 'ActionGeo_Lat', 'ActionGeo_Long', now I've got
    #  to catch and handle those cases, rare as they may be.
    # B09h
    def llConverter(x):
      '''Catches and removes '#' values which happen to be messing up
conversion to float for 'events' Latitude/Longitude field values.
      '''
      if not pd.isnull(x):
        if '#' in x:
          x = x.replace('#', '')
        return float(x)
      else:
        return None

    # per-table post-read column conversions
    if table == 'events':
      if verbose:
        print("Lat/Lon ", end = '')
      # See description
      cleanDF['Actor1Geo_Lat'] = cleanDF['ActionGeo_Lat'].apply(llConverter)
      cleanDF['Actor1Geo_Long'] = cleanDF['ActionGeo_Long'].apply(llConverter)
      cleanDF['Actor2Geo_Lat'] = cleanDF['ActionGeo_Lat'].apply(llConverter)
      cleanDF['Actor2Geo_Long'] = cleanDF['ActionGeo_Long'].apply(llConverter)
      cleanDF['ActionGeo_Lat'] = cleanDF['ActionGeo_Lat'].apply(llConverter)
      cleanDF['ActionGeo_Long'] = cleanDF['ActionGeo_Long'].apply(llConverter)
      if verbose:
        print("D.")
      cleanDF["DATEADDED"] = cleanDF["DATEADDED"].apply(dtConverter)
      #   Events also requires parsing of various fields' coded values, though
      # the increase in disk space and RAM required for expansion of those
      # values into legible strings may be too much prior to EDA

    elif table == 'gkg':
      if verbose:
        print("D.")
      try:
        #   This dropna is unfortunately necessary due to GKG's V2EXTRASXML
        # field values occasionally mixing newline characters into the
        # PAGE_LINKS subfield, albeit only rarely, resulting in junk rows that
        # Pandas is apparently failing to discard...
        cleanDF = cleanDF.dropna(axis = 0, thresh = 5)
        #   Even this dropna() has no apparent effect, as this line's raised
        # exception reveals junk rows in almost 20 files across the 31 day test
        # batch. 20 out of 2758 GKG giles (8274 across all tables) isn't that
        # bad, so I've corrected the junk rows by hand.
        cleanDF["V21DATE"] = cleanDF["V21DATE"].apply(dtConverter)
        #   My apologies for not properly handling this rare bug-case on the
        # part of that specific portion of GKG record formatting. The field
        # isn't even providing any tangible value, given its description...
      except ValueError as e:
        print("Error: %s" % (e))
        return False

    elif table == 'mentions':
      if verbose:
        print("ETD/MTD.")
      cleanDF["EventTimeDate"] = cleanDF["EventTimeDate"].apply(dtConverter)
      cleanDF["MentionTimeDate"] =cleanDF["MentionTimeDate"].apply(dtConverter)

    # branching for GDELTeda.realtimeEDA() use
    if mode == 'batch':
      os.chdir(self.toolData['path'][table]['clean'])
    elif mode == 'realtime':
      os.chdir(self.toolData['path'][table]['realtimeC'])
    cleanDF.to_json(cleanFileName, orient = 'records', date_format = 'iso',
                   date_unit = 'us')

    # optional raw file deletion for space saving, default false
    if deleteRaw:
      try:
        if mode == 'batch':
          os.chdir(self.toolData['path'][table]['raw'])
          os.remove(rawFilePath)
          self.localFiles[table]['raw'].remove(rawFileName)
        elif mode == 'realtime':
          os.chdir(self.toolData['path'][table]['realtimeR'])
          os.remove(rawFilePath)
          self.localFiles[table]['realtimeR'].remove(rawFileName)
      except OSError as e:
        print("Error: %s : %s" % (rawFilePath, e.strerror))
        return False

    if mode == 'batch':
      self.localFiles[table]['clean'].append(cleanFileName)
    elif mode == 'realtime':
      self.localFiles[table]['realtimeC'].append(cleanFileName)
    return True


  # B10
  def cleanTable(self, table, deleteRaw = False, verbose = False):
    '''Iterates over all local 'raw' files present for a table, parsing
 and cleaning each.

Parameters:
----------

table - string
  Must be one of ['events', 'gkg', 'mentions'], controls how files will
 be parsed and cleaned.

deleteRaw - boolean, default False
  Passed to each cleanFile() call, makes deletion of raw files optional.

verbose - boolean, default False
  Controls printed output, default prints counts for files cleaned or
 skipped). If true, prints fileName and conversions applied for each
 file cleaned, or else that a file is already cleaned and therefore
 skipped.

output:
------

Stores reduced-size, parsed/cleaned-contents copy in 'clean' directory
appropriate to the table of each 'raw' file, and displays counts of
files cleaned or skipped upon completion.
See parameter description for 'verbose' for printed output.
    '''

    if table not in ['events', 'gkg', 'mentions']:
      print("Bad 'table' value, specify one of 'events', 'gkg', 'mentions'.")
      return
    else:
      timecheck = time()
      filesCleaned = 0
      filesSkipped = 0

      tablePath = self.toolData['path'][table]['raw']
      os.chdir(tablePath)

      print("Starting to clean any raw", table, "files...")
      localSnapShot = self.localFiles[table]['raw'][:]

      if len(localSnapShot) == 0:
        print("No raw files present for table '%s'." % (table))
        return
      else:
        for fileName in localSnapShot:
          cleaned = self.cleanFile(fileName, deleteRaw, verbose)
          if cleaned:
            filesCleaned += 1
          else:
            filesSkipped += 1
        print("Finished, took %0.3f seconds." % \
              (float(time())-float(timecheck)))
        print("%d files cleaned, %d files skipped." % \
              (filesCleaned, filesSkipped))


  # B11
  def mongoFile(self, fileName, table, verbose = False, mode = 'batch'):
    '''Loads cleaned GDELT .json files/records, then inserts the records
into a table and mode-appropriate MongoDB collection.

Parameters:
----------

fileName - string
  formatted according to table type (extension).

table - string
  Must be one of ['events', 'gkg', 'mentions'] (checked in mongoTable),
controls what table will have its clean files exported to mongoDB.

verbose - boolean, default False
  Controls printed output, default 'terse' (no output in this method,
mongoTable will still print counts).
If true, prints each fileName and the count of records exported, or else
that a file has been skipped (file not clean or not found).

mode - string, default 'batch'
   Controls function behavior, default is for calls by
 GDELTbase.mongoTable() for mass-export of GDELT datafiles. String
 'realtime' is passed for calls in GDELTeda.realtimeEDA(), in which
 individual files are downloaded and cleaned in separate directories and
 exported and profiled individually.

output:
------

Records added to mongoDB instance.

See parameter description for 'verbose' for printed output.

Returns count of records processed and written, or boolean False upon
failure, intended for handling in mongoTable() to track export results.
    '''
    if mode == 'batch':
      if fileName not in self.localFiles[table]['clean']:
        if verbose:
          print(fileName, "is not clean! Skipping...")
        return False
      else:
        if verbose:
          print("  Trying %s..." % (fileName), end='')
        filePath = os.path.join(self.toolData['path'][table]['clean'],
                                fileName)
        with open(filePath) as f:
          exportData = json.load(f)
        recordsCount = len(exportData)
        if not recordsCount < 1:
          if verbose:
            print("loaded. Exporting...", end='')
          try:
            self.localDb['collections'][table].insert_many(exportData)
            if verbose:
              print("success!")
            return recordsCount
          except pymongo.errors.ServerSelectionTimeoutError as sste:
            print(sste)
            return False
        else:
          if verbose:
            print("json.load() failure! Skipping...")
          return False
    # this feels like a sloppy implementation, but I'm pretty short on time.
    elif mode == 'realtime':
      if fileName not in self.localFiles[table]['realtimeC']:
        if verbose:
          print(" ",fileName, "is not clean! Skipping...")
        return False
      else:
        if verbose:
          print("  Trying %s..." % (fileName), end='')
        filePath = os.path.join(self.toolData['path'][table]['realtimeC'],
                                fileName)
        with open(filePath) as f:
          exportData = json.load(f)
        recordsCount = len(exportData)
        if not recordsCount < 1:
          if verbose:
            print("loaded. Exporting...", end='')
          try:
            self.localDb['collections']['realtime'][table].insert_many(
              exportData)
            if verbose:
              print("success!")
            return recordsCount
          except pymongo.errors.ServerSelectionTimeoutError as sste:
            print(sste)
            return False
        else:
          if verbose:
            print("json.load() failure! Skipping...")
          return False

  # B12
  def mongoTable(self, table, reindex = False, verbose = False):
    '''Iterates over all local 'clean' files present for a table,
calling mongoFile() on each.

Parameters:
----------

table - string
  Must be one of ['events', 'gkg', 'mentions'], controls what table will
have its clean files exported to mongoDB

reindex - boolean, default False
  Controls post-export behavior. If True, MongoDB instance will reindex
the appropriate 'table'-parameter-controlled MongoDB collection wildcard
text indexes to accomodate new records.

verbose - boolean, default False
  Passed to internal calls on self.mongoFile().
  Controls printed output, default 'terse' (prints counts for files and
records exported to MongoDB).
If true, prints each fileName and number of records exported, or else
skipped due to lack of cleaning or 'file not found'.

output:
------

Records added to mongoDB instance.

See parameter description for 'verbose' for printed output.
    '''

    timecheck = time()
    totalFiles = 0
    totalRecords = 0
    if table not in ['events', 'gkg', 'mentions']:
      print("Bad 'table' value, specify one of 'events', 'gkg', 'mentions'.")
      return
    elif len(self.localFiles[table]['clean']) < 1:
      print("No clean files found for '%s' table, aborting..." % table)
      return
    else:
      tablePath = self.toolData['path'][table]['clean']
      os.chdir(tablePath)
      print("Exporting %d '%s' files to MongoDB..." % \
            (len(self.localFiles[table]['clean']), table))
      for fileName in self.localFiles[table]['clean']:
        success = self.mongoFile(fileName, table, verbose)
        if success:
          totalFiles += 1
          totalRecords += success
        else:
          continue
      print("MongoDB import complete,",
            "%d files with %d records added to table '%s' (%0.3f seconds)" % \
            (totalFiles,totalRecords,table,(float(time())-float(timecheck))))
      if reindex:
        timecheck = time()
        print("Rebuilding '%s' table indexes (may take a while)..." % (table),
              end='')
        self.localDb['collections'][table].reindex()
        print("done. (%0.3fs)" % (float(time())-float(timecheck)))

# C00
# in-design iterative testing with direct execution
if __name__ == "__main__":
  print("####################################################################")
  print("##########               GDELTbase testing                ##########")
  print("####################################################################")
  
  # Initial object functionality testing...

  print("\nInstantiating Gbase...")
  Gbase = GDELTbase()

  print("\nTrying showLocalFiles()...")
  Gbase.showLocalFiles()

  ''' Preserving old tests in case of later need, reference by end users.

  print("\n------------------------------------------------------------------")

  # Use case: you need to download, preprocess, and store a lot of GDELT data.

  # Downloading up to 93 datafiles per-table, per-day:

  print("\nTesting downloadGDELTDay()...")

  Gbase.downloadGDELTDay('2021/06/25', 'events')
  Gbase.downloadGDELTDay('2021/06/25', 'gkg')
  Gbase.downloadGDELTDay('2021/06/25', 'mentions')

  # Automatically cleaning raw GDELT records to JSON files for export:

  print("\nTesting cleanTable()...")

  Gbase.cleanTable('events')
  Gbase.cleanTable('gkg')
  Gbase.cleanTable('mentions')

  #   Automatically batch-exporting all currently acquired datafiles, to
  # MongoDB collections, per-table:

  print("\nTesting mongoTable()...")

  Gbase.mongoTable('events')
  Gbase.mongoTable('gkg')
  Gbase.mongoTable('mentions')

  #   Batch EDA methods (experimental) may be found in GDELTeda.py, along with
  # realtime EDA methods. GDELTbase methods need not be called directly for
  # them to see use in GDELTeda.realtimeEDA(), automated generation of EDA
  # profiles for current GDELT update datafiles.

  print("\n------------------------------------------------------------------")
  
  #   Verifying column datatype mapping and record integrity, per-table...

  #   The remaining sections of code are intended to provide some insight on
  # basic handling of GDELT data, given GDELTbase-provided mappings, and
  # utility functions from Pandas.

  print("\nChecking record parsing results...\n")

  # Events record parsing

  # replace this string with the known filename of a raw GDELT file currently
  # found in the appropriate project directory.
  eFN = '20210513220000.export.CSV'

  os.chdir(Gbase.toolData['path']['events']['raw'])

  print("Testing file cleaning for events...")

  Gbase.cleanFile(eFN)

  os.chdir(Gbase.toolData['path']['events']['clean'])

  print("Reading cleaned file...\n")

  eDf = pd.read_csv(
    eFN, sep = '\t', dtype = Gbase.toolData['columnTypes']['events'],
    converters = {'DATEADDED': (lambda x: pd.to_datetime(str(x),
                                                         format="%Y%m%d%H%M%S",
                                                         errors='ignore'))}
    )

  print("Checking re-read cleaned file column datatypes...\n")

  pp(eDf.dtypes)

  print("")

  eDict = eDf.to_dict(orient = 'records')

  print("Record 0:")

  pp(eDict[0])

  print("Record 1:")

  pp(eDict[1])

  
  print("\n------------------------------------------------------------------")

  # Global Knowledge Graph record parsing

  # replace this string with the known filename of a raw GDELT file currently
  # found in the appropriate project directory.
  gFN = '20210513220000.gkg.csv'

  os.chdir(Gbase.toolData['path']['gkg']['raw'])

  print("Testing file cleaning for gkg...")

  Gbase.cleanFile(gFN)

  os.chdir(Gbase.toolData['path']['gkg']['clean'])

  print("Reading cleaned file...\n")

  gDf = pd.read_csv(
    gFN, sep = '\t', dtype = Gbase.toolData['columnTypes']['gkg'],
    converters = {'V21DATE': (lambda x: pd.to_datetime(str(x),
                                                         format="%Y%m%d%H%M%S",
                                                         errors='ignore'))}
    )

  print("Checking re-read cleaned file column datatypes...\n")

  pp(gDf.dtypes)

  print("")

  gDict = gDf.to_dict(orient = 'records')

  print("Record 0:")

  pp(gDict[0])

  print("Record 1:")

  pp(gDict[1])


  print("\n------------------------------------------------------------------")
  
  # Mentions record parsing

  # replace this string with the known filename of a raw GDELT file currently
  # found in the appropriate project directory.
  mFN = '20210513220000.mentions.CSV'

  os.chdir(Gbase.toolData['path']['mentions']['raw'])
  
  print("Testing file cleaning for mentions...")
  
  Gbase.cleanFile(mFN)
  
  os.chdir(Gbase.toolData['path']['mentions']['clean'])
  
  print("Reading cleaned file...\n")
  
  mDf = pd.read_csv(
    mFN, sep = '\t', dtype = Gbase.toolData['columnTypes']['mentions'],
    converters = {'EventTimeDate': (lambda x: pd.to_datetime(str(x),
                                                         format="%Y%m%d%H%M%S",
                                                         errors='ignore')),
                  'MentionTimeDate': (lambda x: pd.to_datetime(str(x),
                                                         format="%Y%m%d%H%M%S",
                                                         errors='ignore'))}
    )
  
  print("Checking re-read cleaned file column datatypes...\n")
  
  pp(mDf.dtypes)
  
  print("")
  
  mDict = mDf.to_dict(orient = 'records')
  
  print("Record 0:")
  
  pp(mDict[0])
  
  print("Record 1:")
  
  pp(mDict[1])

  print("\n------------------------------------------------------------------")

  '''

