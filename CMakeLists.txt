cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(pg C)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

# Find Tarantool
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

# Find Postgrsql
set(PostgreSQL_FIND_REQUIRED ON)
find_package(PostgreSQL)
include_directories(${PostgreSQL_INCLUDE_DIRS})
link_directories(${PostgreSQL_LIBRARY_DIRS})

# Set CFLAGS
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")

if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -undefined dynamic_lookup")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -undefined dynamic_lookup")
endif()

# Build module
add_subdirectory(pg)

add_custom_target(check
    COMMAND ${PROJECT_SOURCE_DIR}/test/pg.test.lua)
