# Author: sum01 <sum01@protonmail.com>
# Git: https://github.com/sum01/FindBerkeleyDB
# Read the README.md for the full info.

# NOTE: If Berkeley DB ever gets a Pkg-config ".pc" file, add pkg_check_modules() here

# Checks if environment paths are empty, set them if they aren't

set(_HINTS_PATHS "/usr/local/Cellar/berkeley-db@4")

# Find includes path
find_path(BerkeleyDB_INCLUDE_DIRS
	NAMES "db.h"
	HINTS ${_HINTS_PATHS}
	PATH_SUFFIXES "include"
)


# Checks if the version file exists, save the version file to a var, and fail if there's no version file
if(BerkeleyDB_INCLUDE_DIRS)
	# Read the version file db.h into a variable
	file(READ "${BerkeleyDB_INCLUDE_DIRS}/db.h" _BERKELEYDB_DB_HEADER)
	# Parse the DB version into variables to be used in the lib names
	string(REGEX REPLACE ".*DB_VERSION_MAJOR	([0-9]+).*" "\\1" BerkeleyDB_VERSION_MAJOR "${_BERKELEYDB_DB_HEADER}")
	string(REGEX REPLACE ".*DB_VERSION_MINOR	([0-9]+).*" "\\1" BerkeleyDB_VERSION_MINOR "${_BERKELEYDB_DB_HEADER}")
	# Patch version example on non-crypto installs: x.x.xNC
	string(REGEX REPLACE ".*DB_VERSION_PATCH	([0-9]+(NC)?).*" "\\1" BerkeleyDB_VERSION_PATCH "${_BERKELEYDB_DB_HEADER}")
else()
	if(BerkeleyDB_FIND_REQUIRED)
		# If the find_package(BerkeleyDB REQUIRED) was used, fail since we couldn't find the header
		message(FATAL_ERROR "Failed to find Berkeley DB's header file \"db.h\"! Try setting \"BerkeleyDB_ROOT_DIR\" when initiating Cmake.")
	elseif(NOT BerkeleyDB_FIND_QUIETLY)
		message(WARNING "Failed to find Berkeley DB's header file \"db.h\"! Try setting \"BerkeleyDB_ROOT_DIR\" when initiating Cmake.")
	endif()
	# Set some garbage values to the versions since we didn't find a file to read
	set(BerkeleyDB_VERSION_MAJOR "0")
	set(BerkeleyDB_VERSION_MINOR "0")
	set(BerkeleyDB_VERSION_PATCH "0")
endif()

# The actual returned/output version variable (the others can be used if needed)
set(BerkeleyDB_VERSION "${BerkeleyDB_VERSION_MAJOR}.${BerkeleyDB_VERSION_MINOR}.${BerkeleyDB_VERSION_PATCH}")

# Finds the target library for berkeley db, since they all follow the same naming conventions
macro(findpackage_berkeleydb_get_lib _BERKELEYDB_OUTPUT_VARNAME _TARGET_BERKELEYDB_LIB)
	# Different systems sometimes have a version in the lib name...
	# and some have a dash or underscore before the versions.
	# CMake recommends to put unversioned names before versioned names
	find_library(${_BERKELEYDB_OUTPUT_VARNAME}
		NAMES
		"${_TARGET_BERKELEYDB_LIB}"
		"lib${_TARGET_BERKELEYDB_LIB}"
		"lib${_TARGET_BERKELEYDB_LIB}${BerkeleyDB_VERSION_MAJOR}.${BerkeleyDB_VERSION_MINOR}"
		"${_TARGET_BERKELEYDB_LIB}-${BerkeleyDB_VERSION_MAJOR}.${BerkeleyDB_VERSION_MINOR}"
		"lib${_TARGET_BERKELEYDB_LIB}_${BerkeleyDB_VERSION_MAJOR}.${BerkeleyDB_VERSION_MINOR}"
		"lib${_TARGET_BERKELEYDB_LIB}${BerkeleyDB_VERSION_MAJOR}${BerkeleyDB_VERSION_MINOR}"
		"lib${_TARGET_BERKELEYDB_LIB}-${BerkeleyDB_VERSION_MAJOR}${BerkeleyDB_VERSION_MINOR}"
		"lib${_TARGET_BERKELEYDB_LIB}_${BerkeleyDB_VERSION_MAJOR}${BerkeleyDB_VERSION_MINOR}"
		"lib${_TARGET_BERKELEYDB_LIB}${BerkeleyDB_VERSION_MAJOR}"
		"lib${_TARGET_BERKELEYDB_LIB}-${BerkeleyDB_VERSION_MAJOR}"
		"lib${_TARGET_BERKELEYDB_LIB}_${BerkeleyDB_VERSION_MAJOR}"
		HINTS ${_HINTS_PATHS}
	)
	# If the library was found, add it to our list of libraries
	if(${_BERKELEYDB_OUTPUT_VARNAME})
		# If found, append to our libraries variable
		# The ${{}} is because the first expands to target the real variable, the second expands the variable's contents...
		# and the real variable's contents is the path to the lib. Thus, it appends the path of the lib to BerkeleyDB_LIBRARIES.
		list(APPEND BerkeleyDB_LIBRARIES "${${_BERKELEYDB_OUTPUT_VARNAME}}")
	endif()
endmacro()

# Find and set the paths of the specific library to the variable
findpackage_berkeleydb_get_lib(BerkeleyDB_LIBRARY "db")
# NOTE: Windows doesn't have a db_cxx lib, but instead compiles the cxx code into the "db" lib
findpackage_berkeleydb_get_lib(BerkeleyDB_Cxx_LIBRARY "db_cxx")

# Needed for find_package_handle_standard_args()
include(FindPackageHandleStandardArgs)
# Fails if required vars aren't found, or if the version doesn't meet specifications.
find_package_handle_standard_args(BerkeleyDB
	FOUND_VAR BerkeleyDB_FOUND
	REQUIRED_VARS
		BerkeleyDB_INCLUDE_DIRS
		BerkeleyDB_LIBRARY
	VERSION_VAR BerkeleyDB_VERSION
)

# Create an imported lib for easy linking by external projects
if(BerkeleyDB_FOUND AND BerkeleyDB_LIBRARIES AND NOT TARGET Oracle::BerkeleyDB)
	add_library(Oracle::BerkeleyDB UNKNOWN IMPORTED)
	set_target_properties(Oracle::BerkeleyDB PROPERTIES
		INTERFACE_INCLUDE_DIRECTORIES "${BerkeleyDB_INCLUDE_DIRS}"
		IMPORTED_LOCATION "${BerkeleyDB_LIBRARY}"
		INTERFACE_LINK_LIBRARIES "${BerkeleyDB_LIBRARIES}"
	)
endif()

# Only show the includes path and libraries in the GUI if they click "advanced".
# Does nothing when using the CLI
mark_as_advanced(FORCE
	BerkeleyDB_INCLUDE_DIRS
	BerkeleyDB_LIBRARIES
	BerkeleyDB_LIBRARY
	BerkeleyDB_Cxx_LIBRARY
)

include(FindPackageMessage)
# A message that tells the user what includes/libs were found, and obeys the QUIET command.
find_package_message(BerkeleyDB
	"Found BerkeleyDB libraries: ${BerkeleyDB_LIBRARIES}"
	"[${BerkeleyDB_LIBRARIES}[${BerkeleyDB_INCLUDE_DIRS}]]"
)
