## Project: WGU Data Management/Analytics Undergraduate Capstone
#### A Simplified Pipeline for GDELT Media Analysis

Richard Smith
September 2021

---

Hi there. I needed to create a simplified pipeline for GDELT news media analysis as part of my capstone project for a degree in Data Management/Analytics with Western Governors University. So, given a lot of time and effort, I'd like to provide the scripted tools I needed to develop for that pipeline for public use. Here's hoping other students or individuals will find some use in the cluttered, often needlessly detailed commentary provided in each script's documentation of key functions and processes in acquisition, preprocessing, storage, querying, and simple EDA on GDELT records for Events/Mentions and GKG--notably excepting GKG's GCAM measures, which were considered outside the scope of the project.

---

Contents:
- 000 - Dedication
- 001 - Disclaimer
- A00 - Python version, MongoDB, and Python packages required.
 - A01 - General design structure, intent, and documentation provided.
- B00 - files
 - B01 - GDELTbase.py
 - B02 - GDELTjob.py
 - B03 - GDELTeda.py
 - B04 - GDELTedaGKGhelpers.py
- C00 - Basic uses
 - C01 - Mass download, preprocessing, and storage of GDELT files
 - C02 - Realtime download, preprocessing, and storage of GDELT files
 - C03 - Batch EDA on GDELT records
 - C04 - Realtime EDA on GDELT records
- D00 - Potential for improvements

---

000 - Dedication

  To my partner Cathy for being an ever-present source of support and care in my life, and throughout the design and implementation of this project.
  To my family and friends, for much the same reasons.
  To all of my teachers, Piedmont Virginia Community College, and Western Governors University for all of the time and care that has been taken with my education on so many fronts.
  To the study of the world and its data, and the never-ending search for reasons and ways to do more good for more life, everywhere.

---

001 - Disclaimer

  I, Richard Smith, make no claims of support, communication, or acknowledgment of this project and/or its scripted results by either GDELT Project (the data set used) or any of the creators, maintainers, or licensors of the packages this project requires for dependencies.

Anyone who seeks to make personal use of these tools, or any results provided by those tools, does so without any guarantee for their quality or contents, or any acceptance of liability on the part of the author of this project.

The end goal of these scripts and instructions is the provision of a simplified pipeline for GDELT data analysis, meaning automated downloading, preprocessing, storage, retrieval, and generation of Pandas Profiling ProfileReport html documents for exploratory data analysis (EDA). As such, few class methods have been provided beyond that scope.

---

A00 - Python version, MongoDB, and Python packages required.

  The Python version used in this project is 3.9.6, but so long as care is taken with maintaining compatibility with the packages specified in requirements.txt, all scripts should be compatible with most Python versions at least greater than 3, though testing for that compatibility has not been performed, and no guarantee of operations under other versions of Python are provided or intended.

A local instance of MongoDB will need to be installed under default configuration, or such configuration that it will remain compatible with the scripts which will need to interface with it. At the time of this writing, MongoDB is available for unpaid use outside the context of business and profits, at least, so a visit to their homepage at its current location should suffice for acquiring and installing an unpaid instance of MongoDB.

The simplest method for installation of required packages is by making use of requirements.txt, present here.
Casual searches for syntax will provide this snippet: `pip install -r requirements.txt`, commonly used.

---

A01 - General design structure, intent, and documentation provided.

  The majority of the source code provided in Python scripts is procedural, with standardized operations being applied to varying sets of input, and most operations being distributed over multiple component functions.

  The intent in the design of these scripts is to promote easy comprehension of the methods applied, and in that spirit a somewhat excessive amount of documentation is provided for each class and function, but also throughout key components of each, such that a casual reader or user of the scripts may easily determine what portions are reponsible for what processes.

---

B00 - files

  In the following section, all Python scripts are described according to their class and member function contents, according to their intended uses.

  This section is not intended as a replacement for documentation contained within each function, but as a relatively high-level overview of all classes and functions, in order to provide a fast entry-point to understanding the contents of each file, and basic uses intended.

  For clarity, class member functions prior to those listed for GDELTeda.py are described with truncated forms of their inline documentation. Class member functions for GDELTeda.py and GDELTgkgHelpers.py are described with the initial portions of their inline documentation, due to the complexity of the material covered there.

  Be advised that many operations rely on the chained results of several functions, or on the precise formatting of certain parameters. Those qualities are discussed within the documentation provided in each script, and should be seen as relatively necessary due to the qualities of GDELT data files, updates, and records.

---

B01 - GDELTbase.py

  This script defines class GDELTbase, initialization of which will result in creation of directory 'GDELTdata' within the main project directory, and subdirectories for several varieties of datafiles, per-table.

GDELTbase requires no parameters for initialization.

GDELTbase class member functions:

- updateLocalFilesIndex()  
    updates instanced tracking of current files within project directories

- clearLocalFilesIndex()
    clears instanced tracking of current files within project directories

- showLocalFiles()
    shows current files within project directories

- wipeLocalFiles()
    deletes user-specified varieties of files within project directories

- extensionToTableName()
    takes a file name of a particular format, returns its associated table

- isFileDownloaded()
    takes a file name of a particular format, returns boolean for file presence

- downloadGDELTFile()
    downloads a specified raw file to project directories

- downloadGDELTDay()
    downloads a specified day and table's GDELT files (up to 93 / day)

- cleanFile()
    takes a file name of a particular format, preprocesses and exports clean

- cleanTable()
    applies cleanFile() for all local raw files of a specified table

- mongoFile()
    takes a file name of a particular format, exports cleaned file to MongoDB

- mongoTable()
    applies mongoFile() for all local cleaned files of a specified table

Please see GDELTbase.py and its documentation per-function for details regarding operations and parameters required for their use.

-------------------------------------------------------------------------------

B02 - GDELTjob.py

  This script defines class GDELTjob, initialization of which will result in instancing of a GDELTbase object, whose member functions are used by GDELTjob member functions for automating mass-download, preprocessing, and storage of GDELT data files for later use in batch EDA.
  
GDELTjob requires this parameter for initialization:

- dateList - list of strings
    Format is 'MM/DD/YYYY' for all dates. Be aware that GDELT's 2.0/2.1 versions of Events/Mentions and GKG datafiles are available no further in the past than January 2017, which means requests for earlier files may result in `404 - file not found` errors and skipped downloads.

GDELTjob may take these additional parameters for initialization:

- tableList - list of strings, default ['events','mentions','gkg']
    Permits limiting the datafiles acquired for a given time period.

- verbose - Boolean, default False
    Controls output for GDELTbase operations. Permits observation of any failed downloads, cleans, or MongoDB exports for later correction.

These are GDELTjob's class member functions:

- download()
    For each date in dateList, GDELTbase.downloadGDELTDay() is applied.

- clean()
    For each table in tableList, GDELTbase.cleanTable() is applied.

- store()
    For each table in tableList, GDELTbase.mongoTable() is applied.

-------------------------------------------------------------------------------

B03 - GDELTeda.py

  This script defines class GDELTeda, initialization of which will result in instancing of a GDELTbase object, whose member functions are used by GDELTjob member functions for automating limited downloading, preprocessing, and storage of GDELT data files for realtime EDA, and in creation of directory EDAlogs in the main project directory, with subdirectories per-table, per-variety of EDA (batch or realtime).

GDELTeda may take this parameter for initialization:

- tableList - list of strings, default ["events", "mentions", "gkg"]
    Controls detection and creation of .../EDALogs/... subdirectories for collection of Pandas Profiling ProfileReport HTML EDA document output. Also controls permission for class member functions to perform operations on tables specified by those functions' tableList parameter, as a failsafe against a lack of project directories required for those operations, specifically output of HTML EDA documents.

GDELTeda class member functions:

- batchEDA()
    Reshapes and re-types GDELT records for generating Pandas Profiling ProfileReport()-automated, exhaustive EDA reports from Pandas DataFrames from MongoDB-query-cursors. WARNING: extremely RAM, disk I/O, and processing intensive. Be aware of what resources are available for these operations at runtime.
	
	Relies on Python multiprocessing.Pool.map() calls against class member functions eventsBatchEDA() and mentionsBatchEDA(), and a regular call on gkgBatchEDA(), which uses multiprocessing.Pool.map() calls within it.

- eventsBatchEDA()
    Performs automatic EDA on GDELT Events record subsets. See function batchEDA() for "if table == 'events':" case handling and how this function is invoked as a multiprocessing.Pool.map() call, intended to isolate its RAM requirements for deallocation upon Pool.close().
	
    In its current state, this function can handle collections of GDELT Events records up to at least the size of the batch EDA test subset used in this capstone project, the 30 day period from 05/24/2020 to 06/22/2020.

- mentionsBatchEDA()
    Performs automatic EDA on GDELT Mentions record subsets. See function batchEDA() for "if table == 'mentions':" case handling and how this function is invoked as a multiprocessing.Pool.map() call, intended to isolate its RAM requirements for deallocation upon Pool.close().
	
    In its current state, this function can handle collections of GDELT Mentions records up to at least the size of the batch EDA test subset used in this capstone project, the 30 day period from 05/24/2020 to 06/22/2020.

- gkgBatchEDA()
    Performs automatic EDA on GDELT Global Knowledge Graph (GKG) record subsets.
    Makes use of these helper functions for multiprocessing.Pool().map() calls, from GDELTedaGKGhelpers.py:
 - pullMainGKGcolumns()
 - applyDtypes()
 - convertDatetimes()
 - convertGKGV15Tone()
 - mainReport()
 - locationsReport()
 - countsReport()
 - themesReport()
 - personsReport()
 - organizationsReport()

  The intent behind this implementation is to reduce the total amount of RAM required for intermediary operations, as calling `.close()` on appropriate process pools should result in deallocation of their memory structures. Well-known issues with Pandas-internal treatment of allocation and deallocation of DataFrames, regardless of whether all references to a DataFrame are passed to 'del' statements, restrict completion of the processing necessary for normalization of all GKG columns, which is itself necessary for execution of EDA on all columns. The apparent RAM requirements for those operations on the batch test GKG data set are not mitigable under hardware circumstances for the author.

  The uncommented code in this function represents a working state which can produce full batch EDA on at least the primary information-holding GKG columns, but not for the majority of variable-length and subfielded columns. V1Locations batch EDA has been produced from at least one attempt, but no guarantee of its error-free operation on similarly-sized subsets of GDELT GKG records is intended or encouraged.

- realtimeEDA()
    Performs automatic EDA on the latest GDELT datafiles for records from Events/Mentions and GKG. This function is enabled by loopEDA() to download a specified window of datafiles, or else just most-recent datafiles if called by itself, or for a default loopEDA() call.
    Current testing on recent GDELT updates confirms that this function may complete all EDA processing on each datafile set well within the fifteen minute window before each successive update.

- loopEDA()
    Loops calls of realtimeEDA() N iterations, where N is equal to parameter window multiplied appropriately for parameter windowUnit. Handles various potential failure states and delays execution of iterations until each next set of datafiles should be available.
	Note: this function has specifically been designed to collect current and future GDELT datafiles, rather than collecting specified windows within however many updates prior to the current update, as that functionality can already be achieved with GDELTbase.py or GDELTjob.py class member functions.

  Please see GDELTeda.py and its documentation per-function for details regarding operations and parameters required for their use.

-------------------------------------------------------------------------------

B04 - GDELTedaGKGhelpers.py

  Class for collecting Python.multiprocessing.Pool.map()-compatible functions for performing RAM-isolated operations on GDELT GKG record subset dataframes.

GDELTedaGKGhelpers does not require initialization for its use in collecting these functions.

GDELTedaGKGhelpers class member functions:

- pullMainGKGcolumns()
    Extracts all non-variable-length columns for GKG records present in local MongoDB instance and converts them into a returned Pandas DataFrame.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.

- applyDtypes()
    Calls tableDF.astype(dtype = columnTypes, copy = False), returns resulting typed DataFrame.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.

- convertDatetimes()
    Calls pd.todatetime(tableDF[datetimeField], datetimeFormat), returns resulting modified DataFrame.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.

- convertGKGV15Tone()
    Converts V15Tone subfield key:value pairs to V15Tone_X columns, returns resulting modified DataFrame.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.

- mainReport()
    Generates basic EDA on GKG columns not subject to variable-length values.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.

- locationsReport()
    Converts V1Locations subfield dict lists to V1Locations_X columns as a DataFrame normalized for variable-length lists of locations, which is then used to generate a Pandas Profiling report.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.
	
	WARNING: the current memory signature of this function's use on 31 days of GDELT GKG records (reported as 4373849 records, expanded to over 16077985 records with explode() on V1Locations, reported size 2.3GB in Pandas DataFrame form) is over 30GB in Python.exe alone. Both RAM and disk I/O will be dominated by these operations for any large GKG subsets.

- countsReport()
    Converts V1Counts subfield dict lists to V1Counts_X columns as a DataFrame normalized for variable-length lists of counts, which is then used to generate a Pandas Profiling report.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.
	
	WARNING: this function is not working for the 30 day batch EDA test subset of GDELT records. As such, it's been left in place as potentially operable for smaller sets, but such testing has not been performed as part of this capstone project.

- themesReport()
    Converts V1Themes subfield lists to individual column values as a DataFrame normalized for variable-length lists of themes, which is then used to generate a Pandas Profiling report.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.
	
	WARNING: this function is not working for the 30 day batch EDA test subset of GDELT records. As such, it's been left in place as potentially operable for smaller sets, but such testing has not been performed as part of this capstone project.

- personsReport()
    Converts V1Persons subfield lists to individual column values as a DataFrame normalized for variable-length lists of persons, which is then used to generate a Pandas Profiling report.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.
	
    WARNING: this function is not working for the 30 day batch EDA test subset of GDELT records. As such, it's been left in place as potentially operable for smaller sets, but such testing has not been performed as part of this capstone project.

- organizationsReport()
    Converts V1Organizations subfield lists to individual column values as a DataFrame normalized for variable-length lists of organizations, which is then used to generate a Pandas Profiling report.
	Intended for use in GDELTeda.py, GDELTeda.gkgBatchEda() multiprocessing.Pool().map() calls.
	
    WARNING: this function is not working for the 30 day batch EDA test subset of GDELT records. As such, it's been left in place as potentially operable for smaller sets, but such testing has not been performed as part of this capstone project.

Please see GDELTedaGKGhelpers.py and its documentation per-function for details regarding operations and parameters required for their use.

---

D00 - Potential for improvements

  Any programmer with more than an arbitrary level of experience would likely scratch their head and wonder why any number of choices were made toward design and completion of this project: it has been designed and implemented by a programmer with less than that arbitrary level of experience, according to the results of their consideration of basic requirements for basic functionality.

  As such, there are many places in these scripts which would benefit from refactoring, reorganization, and redesign. That potential for improvement may be seen as a worthy exercise for most CS students, given the simple structure of the scripts, the relatively low hardware requirements for default behavior of realtimeEDA(), and the somewhat interesting character of the data provided by GDELT for public use. It is in those hopes that the relatively limited sin of releasing an imperfect set of tools is committed.

---

This project is intended for release under the BSD 3-clause open-source license, contingent on admission of no known or claimed approval or support for this project by any of the licensors, developers, or maintainers of any of the open-source packages implemented as component dependencies.
