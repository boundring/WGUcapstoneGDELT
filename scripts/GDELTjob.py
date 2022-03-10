'''GDELTjob.py
Project: WGU Data Management/Analytics Undergraduate Capstone
Richard Smith
August 2021

Class for collecting GDELTbase operations to download, clean, and store
GDELT data.

Contents:
  A00 - GDELTjob
    A01 - __init__ with instanced data
  B00 - class methods
    B01 - download()
    B02 - clean()
    B03 - store()
    B04 - execute()
  C00 - main w/ testing
    C01 - previously-run GDELT data acquisition, preprocessing, storage
'''
from GDELTbase import GDELTbase

# A00 - GDELTjob
class GDELTjob:
  '''Collects GDELTbase operations for downloading, cleaning, and
exporting GDELT files for a user-specified period.

Shared class data:
-----------------

dateList -- list of strings.
  Values must be formatted like 'YYYY/MM/DD'.
Controls what dates will be passed to GDELTbase.downloadGDELTDay().

tableList -- list of strings, default ['events','mentions','gkg'].
  Values must be one or more of ['events', 'gkg', 'mentions'].
GDELTbase can tolerate repeating values in this list, but will be forced
to skip any number of files in downloading, cleaning, and storing.

Instanced class data:
--------------------

Gbase - GDELTbase instance.


Class methods:
-------------

download()
clean()
store()
execute()
  '''

  # A01
  def __init__(self, dateList, tableList = ['events','mentions','gkg'],
               verbose = False):
    '''GDELTjob class initialization, takes list of dates and list of
tables for datafiles to acquire. Instances GDELTbase() object for use by
class methods.

Parameters:
----------

dateList - list of strings
  Format is 'YYYY/MM/DD' for all dates. Be aware that GDELT's 2.0/2.1
versions of Events/Mentions and GKG datafiles are available no further
in the past than January 2017, which means requests for earlier files
may result in '404 - file not found' errors and skipped downloads.

tableList - list of strings, default ['events','mentions','gkg']
  Permits limiting the datafiles acquired for a given time period.

verbose - Boolean, default False
  Controls output for GDELTbase operations. Permits observation of any
failed downloads, cleans, or MongoDB exports for later correction.

output:
------

  See parameter description for 'verbose' for printed output, both here
and in all related GDELTbase methods.
    '''
    self.Gbase = GDELTbase()
    self.dateList = dateList
    self.tableList = tableList
    self.verbose = verbose

  # B00 - class methods

  # B01
  def download(self):
    '''Executes GDELTbase.downloadGDELTDay() for each date and table in
dateList and tableList.
    '''
    for date in self.dateList:
      for table in self.tableList:
        self.Gbase.downloadGDELTDay(date, table, verbose = self.verbose)

  # B02
  def clean(self):
    '''Executes GDELTbase.cleanTable() for all tables.
    '''
    for table in self.tableList:
      self.Gbase.cleanTable(table, verbose = self.verbose)

  # B03
  def store(self):
    '''Executes GDELTbase.mongoTable() for all cleaned files.
    '''
    for table in self.tableList:
      self.Gbase.mongoTable(table, verbose = self.verbose)

  # B04
  def execute(self):
    '''Executes each of download(), clean(), and store() for a given
GDELTjob() instance.
    '''
    self.download()
    self.clean()
    self.store()

# C00
# in-design iterative testing with direct execution
if __name__ == "__main__":
  
  # C01
  ''' This commented-out section is for saving previously-run test code, which
# may be of use in developing an understanding of this class and its methods,
# used to acquire the 30 day batch EDA test set of GDELT Events/Mentions and
# GKG records.

  print("####################################################################")
  print("##########               GDELTjobs testing                ##########")
  print("####################################################################")
  
  # Previously-run GDELT data acquisition, preprocessing, and MongoDB storage
  
  # COMPLETED POST-DEBUG ON 2021-08-22
  # A MONTH OF BLACK LIVES MATTER PROTESTS
  #     Initial batch EDA testing set formed from days before and after
  # reporting of George Floyd's death at the hands of Derek Chauvin, and
  # ensuing coverage of protests and related events throughout the next 30 days

  print("\nPerforming testing for download, cleaning, storage for all tables.")
  tables = ['events', 'mentions', 'gkg']
  
  #     Automating generation of string dates from '2020/05/24' to '2020/06/22'
  # in list form for passing to a GDELTjob instance. This could probably get
  # replaced with a list comprehension and a lambda function.
  jobDates = []
  dateStart = '2020/05/'
  for day in range(24, 32):
    thisday = str(day)
    jobDates.append(dateStart + thisday)
  dateStart = dateStart.replace('5', '6')
  for day in range(1, 23):
    if day < 10:
      thisday = '0' + str(day)
    else:
      thisday = str(day)
    jobDates.append(dateStart + thisday)
  
  gJob = GDELTjob(dateList = jobDates, tableList = tables, verbose = True)

  #   Comment this method call out to permit use of GDELTbase methods against
  # individual files (say, if one or more files fail to be downloaded, cleaned,
  # or stored in MongoDB, and need to be re-tried individually).
  gJob.execute()

  #   POST-DEBUG ON 2021-08-26
  #   These files failed cleaning (XML field issue) and needed to be re-cleaned
  # and stored. See project documentation for details on the issue, occasional
  # newlines within the trailing, otherwise disregarded XML field in GKG
  # records would result in junk records excepting file cleaning and export for
  # invalid field NaNs. They occasionally pop up in GKG, given some other
  # instances of this bug's appearance. I've been opting to simply open those
  # RAW .csv files in a text editor, scroll quickly with a sidebar view of the
  # document to quickly find the problem line, then just delete the newline
  # character, i.e. fix the broken record.
  #   I've had to do this for maybe 20 files from varying time periods, and
  # these are the files that needed to be fixed for this time period. There
  # does not appear to be any explanation besides errant newlines in that XML
  # field, which is just an output bug from whatever portion of GDELT's
  # crawlers that aggregates those long strings.
  tables = ['gkg']
  gJob = GDELTjob(dateList = ['2020/05/24'], tableList = tables, verbose = True)
  # gJob.execute()
  files = [
  '20200608064500.gkg.json',
  '20200608101500.gkg.json',
  '20200620161500.gkg.json',
  ]
  for fileName in files:
    gJob.Gbase.mongoFile(fileName, 'gkg', True)
  '''

