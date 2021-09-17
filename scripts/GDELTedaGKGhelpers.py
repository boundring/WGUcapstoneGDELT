'''GDELTedaGKGhelpers.py
Project: WGU Data Management/Analytics Undergraduate Capstone
Richard Smith
August 2021

  Class for collecting Python.multiprocessing.Pool.map()-compatible
functions for performing RAM-isolated operations on GDELT GKG record
subset dataframes.

Contents:
  A - GDELTedaGKGhelpers
    A01 - __init__ -- empty, unused
  B00 - class methods
    B01 - pullMainGKGcolumns()
    B02 - applyDtypes()
    B03 - convertDatetimes()
    B04 - convertGKGV15Tone()
    B05 - mainReport()
    B06 - locationsReport()
    B07 - countsReport()
    B08 - themesReport()
    B09 - personsReport()
    B10 - organizationsReport()
'''
import pymongo
import pandas as pd
from pandas_profiling import ProfileReport
from pprint import pprint as pp


# A
class GDELTedaGKGhelpers:
  '''Class for collecting Python.multiprocessing.Pool.map()-compatible
 functions for performing RAM-isolated operations on GDELT GKG record
 subset dataframes.
  '''
  # A01
  def __init__(self):
    '''This class collects functions called w/o instance.
    '''
    pass

  # B00 - class methods

  # B01
  def pullMainGKGcolumns(mode = 'batch'):
    '''Extracts all non-variable-length columns for GKG records present
 in local MongoDB instance and converts them into a returned Pandas
 DataFrame. Intended for use in GDELTeda method gkgBatchEDA()
 multiprocessing Pool.map() calls.

Parameters:
---------

mode - arbitrary
  This parameter is included to meet Python multiprocessing.Pool.map()
 function requirements. As such, it it present only to receive a
 parameter determined by map(chunksize = 1), e.g. one iteration of the
 function will execute.
    '''
    columnNames = [
      'GKGRECORDID',
      'V21DATE',
      'V2SourceCommonName',
      'V2DocumentIdentifier',
      'V15Tone',
      ]

    localDb = {}
    localDb['client'] = pymongo.MongoClient()
    localDb['database'] = localDb['client'].capstone
    if mode == 'batch':
      localDb['collection'] = localDb['database'].GDELT.gkg
    elif mode == 'realtime':
      localDb['collection'] = localDb['database'].GDELT.realtime.gkg

    print("    Converting Pymongo result cursor to Python list of dicts...")
    tableList = list(localDb['collection'].find(
      projection = {'GKGRECORDID'          : True,
                    'V21DATE'              : True,
                    'V2SourceCommonName'   : True,
                    'V2DocumentIdentifier' : True,
                    'V15Tone'              : True,
                    '_id'                  : False},
      allow_disk_use = True,
      no_cursor_timeout = True,
      ))

    print("    Converting list of dicts to Pandas DataFrame...")
    tableDF = pd.DataFrame.from_records(tableList.copy(),
                                        columns = columnNames)
    del tableList
    return tableDF


  # B02
  def applyDtypes(mainDF):
    '''Calls mainDF.astype(dtype = columnTypes, copy = False), returns
 resulting typed DataFrame. Intended for use in GDELTeda method
 gkgBatchEDA() multiprocessing Pool.map() calls.
    '''
    columnTypes = {
      'GKGRECORDID'          : pd.StringDtype(),
      'V21DATE'              : pd.StringDtype(),
      'V2SourceCommonName'   : pd.StringDtype(),
      'V2DocumentIdentifier' : pd.StringDtype(),
      }
    return mainDF.astype(dtype = columnTypes, copy = False)


  # B03
  def convertDatetimes(mainDF):
    '''Calls pd.to_datetime(mainDF[datetimeField], datetimeFormat),
 returns resulting modified DataFrame. Intended for use in GDELTeda
 method gkgBatchEDA() multiprocessing Pool.map() calls.
    '''
    datetimeField = "V21DATE"
    datetimeFormat = "%Y-%m-%dT%H:%M:%S.000000Z"
    strftimeFormat = "%Y-%m-%dh%Hm%M"
    mainDF[datetimeField] = pd.to_datetime(mainDF[datetimeField],
                                           format = datetimeFormat)
    return mainDF


  # B04
  def convertGKGV15Tone(mainDF):
    '''Converts V15Tone subfield key:value pairs to V15Tone_X columns,
 returns resulting modified DataFrame. Intended for use in GDELTeda
 method gkgBatchEDA() multiprocessing Pool.map() calls.
    '''
    print("    Normalizing...")
    subcols = pd.json_normalize(mainDF['V15Tone'])

    print("    Renaming subfield columns...")
    subcols.columns = [f"V15Tone_{c}" for c in subcols.columns]

    print("    Dropping old 'V15Tone', joining subfield columns...")
    return mainDF.drop(columns = ['V15Tone']).join(subcols)

  # B05
  def mainReport(mainDF):
    '''Generates simple EDA on GKG columns not subject to variable-
 length values. Intended for use in GDELTeda method gkgBatchEDA()
 multiprocessing Pool.map() calls.
    '''
    configFileName = "GDELTgkgMainEDAconfig_batch.yaml"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    edaLogName = "".join(["GDELT_GKG_main_EDA_",
      mainDF['V21DATE'].min().strftime(strftimeFormat),"_to_",
      mainDF['V21DATE'].max().strftime(strftimeFormat), '.html',
      ])

    print("  File output:", edaLogName, "\n")

    print("    Setting index to 'GKGRECORDID'...")
    #   Not using 'inplace = True' in order to preserve mainDF state,
    # regardless of how Python treats function parameters. Safety.
    thisDF = mainDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                              verify_integrity = False)
    profile = ProfileReport(thisDF, config_file = configFileName)
    profile.to_file(output_file = edaLogName)
    del profile
    del thisDF
    return True


  # B06
  def locationsReport(mainDF):
    '''Converts V1Locations subfield dict lists to V1Locations_X
 columns as a DataFrame normalized for variable-length lists of
 locations, which is then used to generate a Pandas Profiling report.
 Intended for use in GDELTeda method gkgBatchEDA() multiprocessing
 Pool.map() calls.

 WARNING: the current memory signature of this function's use on 30 days
 of GDELT GKG records (reported as 4373849 records, expanded to over
 16077985 records with explode() on V1Locations, reported size 2.3GB in
 Pandas DataFrame form) is over 30GB in Python.exe alone. Both RAM and
 disk I/O will be dominated by these operations for any large GKG
 subsets.
    '''
    localDb = {}
    localDb['client'] = pymongo.MongoClient()
    localDb['database'] = localDb['client'].capstone
    localDb['collection'] = localDb['database'].GDELT.gkg

    print("    Converting Pymongo result cursor to Python list of dicts...")
    locationList = list(localDb['collection'].find(
                                                   
      projection = {'V1Locations' : True, '_id' : False},
      allow_disk_use = True,
      no_cursor_timeout = True,
      ))

    print("    Converting list of dicts to Pandas DataFrame...")
    locationDF = pd.DataFrame.from_records(locationList)
    del locationList
    
    print("    Joining with other columns...")
    locationDF = pd.concat([mainDF, locationDF], axis = 1)

    print("    Performing explode()...")
    locationDF = locationDF.explode('V1Locations')

    print("    Normalizing...")
    subcols = pd.json_normalize(locationDF['V1Locations'])

    print("    Renaming subfield columns...")
    subcols.columns = [f"V1Locations_{c}" for c in subcols.columns]

    print("    Dropping old 'V1Locations', joining subfield columns...")
    locationDF = locationDF.drop(columns = ['V1Locations']).join(
      subcols).astype({'V1Locations_FullName'    : pd.StringDtype(),
                       'V1Locations_CountryCode' : pd.StringDtype(),
                       'V1Locations_ADM1Code'    : pd.StringDtype(),
                       'V1Locations_FeatureID'   : pd.StringDtype(),},
                      copy = False)

    print("\n  GKG Locations DataFrame .info():\n")
    print(locationDF.info())

    configFileName = "GDELTgkgLocationsEDAconfig_batch.yaml"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    print("\n    Generating report...")
    edaDates = "".join([
      locationDF['V21DATE'].min().strftime(strftimeFormat),"_to_",
      locationDF['V21DATE'].max().strftime(strftimeFormat),
      ])
    edaLogName = "".join(["GDELT_GKG_locations_EDA_", edaDates,".html"])
    print("    File output:", edaLogName, "\n")

    #   Using 'inplace = True' here and in all following functions in
    # order to save whatever RAM is possible from doing so, though
    # setting the index repeatedly is not ideal.
    print("    Setting index to 'GKGRECORDID'...")
    locationDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                         inplace = True, verify_integrity = False)
    
    profile = ProfileReport(locationDF, config_file = configFileName)

    print("\n    Generating html from report...")
    profile.to_file(edaLogName)

    del profile
    del locationDF
    return True


  # B07
  def countsReport(mainDF):
    '''Converts V1Counts subfield dict lists to V1Counts_X columns as a
 DataFrame normalized for variable-length lists of counts, which is then
 used to generate a Pandas Profiling report. Intended for use in
 GDELTeda method gkgBatchEDA() multiprocessing Pool.map() calls.

   WARNING: this function is not working for the 30 day batch EDA test
 subset of GDELT records. As such, it's been left in place as
 potentially operable for smaller sets, but such testing has not been
 performed as part of this capstone project.
    '''
    localDb = {}
    localDb['client'] = pymongo.MongoClient()
    localDb['database'] = localDb['client'].capstone
    localDb['collection'] = localDb['database'].GDELT.gkg

    print("    Converting Pymongo result cursor to Python list of dicts...")
    countList = list(localDb['collection'].find(
      projection = {'V1Counts' : True, '_id' : False},
      allow_disk_use = True,
      no_cursor_timeout = True,
      ))

    print("    Converting list of dicts to Pandas DataFrame...")
    countDF = pd.DataFrame.from_records(countList)
    del countList

    print("    Joining with other columns...")
    countDF = pd.concat([mainDF, countDF], axis = 1)

    print("    Performing explode()...")
    countDF = countDF.explode('V1Counts')

    print("    Normalizing...")
    subcols = pd.json_normalize(countDF['V1Counts'])

    print("    Renaming subfield columns...")
    subcols.columns = [f"V1Counts_{c}" for c in subcols.columns]
    subcols.drop(columns = ['V1Counts_LocationLatitude',
                            'V1Counts_LocationLongitude'],
                 inplace = True)

    print("\n    Dropping old 'V1Counts', joining subfield columns...")
    countDF = countDF.drop(columns = ['V1Counts']).join(
      subcols).astype({
        'V1Counts_CountType'           : pd.StringDtype(),
        'V1Counts_ObjectType'          : pd.StringDtype(),
        'V1Counts_LocationFullName'    : pd.StringDtype(),
        'V1Counts_LocationCountryCode' : pd.StringDtype(),
        'V1Counts_LocationADM1Code'    : pd.StringDtype(),
        'V1Counts_LocationFeatureID'   : pd.StringDtype(),
        }, copy = False)

    print("\n  GKG Counts DataFrame .info():\n")
    pp(countDF.info())

    configFileName = "GDELTgkgCountsEDAconfig_batch.yaml"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    print("    Generating report...")
    edaDates = "".join([
      countDF['V21DATE'].min().strftime(strftimeFormat),"_to_",
      countDF['V21DATE'].max().strftime(strftimeFormat),
      ])
    edaLogName = "".join(["GDELT_GKG_count_EDA_", edaDates,".html"])
    print("    File output:", edaLogName, "\n")

    print("    Setting index to 'GKGRECORDID'...")
    countDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                      inplace = True, verify_integrity = False)

    profile = ProfileReport(countDF, config_file = configFileName)
    print("    Generating html from report...")
    profile.to_file(edaLogName)
    del profile
    del countDF
    return True


  # B08
  def themesReport(mainDF):
    '''Converts V1Themes subfield lists to individual column values as
 a DataFrame normalized for variable-length lists of themes, which is
 then used to generate a Pandas Profiling report. Intended for use in
 GDELTeda method gkgBatchEDA() multiprocessing Pool.map() calls.

   WARNING: this function is not working for the 30 day batch EDA test
 subset of GDELT records. As such, it's been left in place as
 potentially operable for smaller sets, but such testing has not been
 performed as part of this capstone project.
    '''
    localDb = {}
    localDb['client'] = pymongo.MongoClient()
    localDb['database'] = localDb['client'].capstone
    localDb['collection'] = localDb['database'].GDELT.gkg

    print("    Converting Pymongo result cursor to Python listdicts...")
    themeList = list(localDb['collection'].find(
      projection = {'V1Themes' : True, '_id' : False},
      allow_disk_use = True,
      no_cursor_timeout = True,
      ))

    print("    Converting list of dicts to Pandas DataFrame...")
    themeDF = pd.DataFrame.from_records(themeList)
    del themeList

    print("    Joining with other columns...")
    themeDF = pd.concat([mainDF, themeDF], axis = 1)

    print("    Performing explode()...")
    themeDF = themeDF.explode('V1Themes')

    print("    Correcting dtypes...")
    themeDF = themeDF.astype({'V1Themes' : pd.StringDtype()}, copy = False)

    print("\n  GKG Themes DataFrame .info():\n")
    pp(themeDF.info())

    configFileName = "GDELTgkgThemesEDAconfig_batch.yaml"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    print("    Generating report...")
    edaDates = "".join([
      themeDF['V21DATE'].min().strftime(strftimeFormat),"_to_",
      themeDF['V21DATE'].max().strftime(strftimeFormat),
      ])
    edaLogName = "".join(["GDELT_GKG_themes_EDA_", edaDates,".html"])
    print("    File to output:", edaLogName, "\n")

    print("    Setting index to 'GKGRECORDID'...")
    themeDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                      inplace = True, verify_integrity = False)

    profile = ProfileReport(themeDF, config_file = configFileName)

    print("    Generating html from report...")
    profile.to_file(edaLogName)
    del profile
    del themeDF
    return True


  # B09
  def personsReport(mainDF):
    '''Converts V1Persons subfield lists to individual column values as
 a DataFrame normalized for variable-length lists of persons, which is
 then used to generate a Pandas Profiling report. Intended for use in
 GDELTeda method gkgBatchEDA() multiprocessing Pool.map() calls.

   WARNING: this function is not working for the 30 day batch EDA test
 subset of GDELT records. As such, it's been left in place as
 potentially operable for smaller sets, but such testing has not been
 performed as part of this capstone project.
    '''
    localDb = {}
    localDb['client'] = pymongo.MongoClient()
    localDb['database'] = localDb['client'].capstone
    localDb['collection'] = localDb['database'].GDELT.gkg

    print("    Converting Pymongo result cursor to Python list...")
    personList = list(localDb['collection'].find(
      projection = {'V1Persons' : True, '_id' : False},
      allow_disk_use = True,
      no_cursor_timeout = True,
      ))

    print("    Converting list of dicts to Pandas DataFrame...")
    personDF = pd.DataFrame.from_records(personList)
    del personList

    print("    Joining with other columns...")
    personDF = pd.concat([mainDF, personDF], axis = 1)

    print("    Performing explode()...")
    personDF = personDF.explode('V1Persons')

    print("    Correcting dtypes...")
    personDF = personDF.astype({'V1Persons' : pd.StringDtype()},copy = False)

    print("\n  GKG Persons DataFrame .info():\n")
    pp(personDF.info())

    configFileName = "GDELTgkgPersonsEDAconfig_batch.yaml"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    print("    Generating report...")
    edaDates = "".join([
      personDF['V21DATE'].min().strftime(strftimeFormat),"_to_",
      personDF['V21DATE'].max().strftime(strftimeFormat),
      ])
    edaLogName = "".join(["GDELT_GKG_persons_EDA_", edaDates,".html"])
    print("    File to output:", edaLogName, "\n")

    print("    Setting index to 'GKGRECORDID'...")
    personDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                       inplace = True, verify_integrity = False)

    profile = ProfileReport(personDF, config_file = configFileName)
    print("    Generating html from report...")
    profile.to_file(edaLogName)
    del profile
    del personDF
    return True


  # B10
  def organizationsReport(mainDF):
    '''Converts V1Organizations subfield lists to individual column values as
 a DataFrame normalized for variable-length lists of organizations, which is
 then used to generate a Pandas Profiling report. Intended for use in
 GDELTeda method gkgBatchEda() multiprocessing Pool.map() calls.

   WARNING: this function is not working for the 30 day batch EDA test
 subset of GDELT records. As such, it's been left in place as
 potentially operable for smaller sets, but such testing has not been
 performed as part of this capstone project.
    '''
    localDb = {}
    localDb['client'] = pymongo.MongoClient()
    localDb['database'] = localDb['client'].capstone
    localDb['collection'] = localDb['database'].GDELT.gkg

    print("    Converting Pymongo result cursor to Python list...")
    orgList = list(localDb['collection'].find(
      projection = {'V1Organizations' : True, '_id' : False},
      allow_disk_use = True,
      no_cursor_timeout = True,
      ))

    print("    Converting list of dicts to Pandas DataFrame...")
    orgDF = pd.DataFrame.from_records(orgList)
    del orgList

    print("    Joining with other columns...")
    orgDF = pd.concat([mainDF, orgDF], axis = 1)

    print("    Performing explode()...")
    orgDF = orgDF.explode('V1Organizations')

    print("    Correcting dtypes...")
    orgDF = orgDF.astype({'V1Organizations' : pd.StringDtype()}, copy = False)

    print("\n  GKG Organizations DataFrame .info():\n")
    pp(orgDF.info())

    configFileName = "GDELTgkgOrganizationsEDAconfig_batch.yaml"
    strftimeFormat = "%Y-%m-%dh%Hm%M"

    print("    Generating report...")
    edaDates = "".join([
      orgDF['V21DATE'].min().strftime(strftimeFormat),"_to_",
      orgDF['V21DATE'].max().strftime(strftimeFormat),
      ])
    edaLogName = "".join(["GDELT_GKG_organizations_EDA_", edaDates,".html"])
    print("    File to output:", edaLogName, "\n")

    print("    Setting index to 'GKGRECORDID'...")
    orgDF.set_index(keys='GKGRECORDID', drop = True, append = False,
                       inplace = True, verify_integrity = False)

    profile = ProfileReport(orgDF, config_file = configFileName)
    print("    Generating html from report...")
    profile.to_file(edaLogName)
    del profile
    del orgDF
    return True
