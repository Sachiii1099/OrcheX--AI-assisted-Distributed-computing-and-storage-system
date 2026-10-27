cmake_minimum_required(VERSION 3.10)
project(NEXUS VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)      # task workloads are meant to be optimised
endif()
add_compile_options(-Wall -Wextra)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})   # build/master, build/worker, build/nexus_ml

find_package(Threads REQUIRED)
find_package(PostgreSQL REQUIRED)    # libpq (Debian/Ubuntu: libpq-dev)

# ---- shared: protocol + message framing + TCP helpers + SHA-256/base64 -------
add_library(nexus_common STATIC common/message.cpp common/sha256.cpp)
target_link_libraries(nexus_common PUBLIC Threads::Threads)

# ---- PostgreSQL data-access layer --------------------------------------------
add_library(nexus_db STATIC database/database.cpp)
target_include_directories(nexus_db PUBLIC ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(nexus_db PUBLIC ${PostgreSQL_LIBRARIES} nexus_common)

# ---- ML: random forest + training pipeline -------------------------------------
add_library(nexus_ml_lib STATIC ml/random_forest.cpp ml/xgb_model.cpp ml/trainer.cpp)
target_link_libraries(nexus_ml_lib PUBLIC nexus_db)

# XGBoost (real dmlc/xgboost, C API). Build it once with ./scripts/install_xgboost.sh; it is found in
#   -DNEXUS_XGBOOST_ROOT=<dir>, $NEXUS_XGBOOST_ROOT, ./third_party/xgboost-install, /usr/local or /usr.
# Without it NEXUS still builds (Random Forest only) and reports "XGBoost not available" - it never fakes it.
set(NEXUS_XGBOOST_ROOT "$ENV{NEXUS_XGBOOST_ROOT}" CACHE PATH "prefix containing include/xgboost and lib/libxgboost.so")
find_path(XGBOOST_INCLUDE_DIR xgboost/c_api.h
          HINTS ${NEXUS_XGBOOST_ROOT} ${CMAKE_SOURCE_DIR}/third_party/xgboost-install /usr/local /usr
          PATH_SUFFIXES include)
find_library(XGBOOST_LIBRARY NAMES xgboost
          HINTS ${NEXUS_XGBOOST_ROOT} ${CMAKE_SOURCE_DIR}/third_party/xgboost-install /usr/local /usr
          PATH_SUFFIXES lib lib64 lib/x86_64-linux-gnu)
if(XGBOOST_INCLUDE_DIR AND XGBOOST_LIBRARY)
  message(STATUS "XGBoost found: ${XGBOOST_LIBRARY}")
  target_compile_definitions(nexus_ml_lib PUBLIC NEXUS_HAVE_XGBOOST=1)
  target_include_directories(nexus_ml_lib PUBLIC ${XGBOOST_INCLUDE_DIR})
  target_link_libraries(nexus_ml_lib PUBLIC ${XGBOOST_LIBRARY})
  get_filename_component(XGBOOST_LIB_DIR ${XGBOOST_LIBRARY} DIRECTORY)
  set(CMAKE_BUILD_RPATH ${XGBOOST_LIB_DIR})            # binaries in build/ find libxgboost.so without LD_LIBRARY_PATH
else()
  message(WARNING "XGBoost NOT found - building with Random Forest only. Run ./scripts/install_xgboost.sh and rebuild.")
endif()

# ---- Master core (library so the tests can link it) ---------------------------
add_library(nexus_master_lib STATIC
  master/master_server.cpp master/worker_manager.cpp master/scheduler.cpp
  master/task_manager.cpp master/storage_manager.cpp master/mapreduce.cpp
  master/ml_manager.cpp master/http_server.cpp master/api.cpp master/benchmark.cpp)
target_link_libraries(nexus_master_lib PUBLIC nexus_common nexus_db nexus_ml_lib)

add_executable(master master/main.cpp)
target_link_libraries(master nexus_master_lib)

# ---- Worker (no database access: only the Master talks to PostgreSQL) ----------
add_library(nexus_worker_lib STATIC worker/cpu_affinity.cpp
  worker/worker_client.cpp worker/resource_monitor.cpp worker/task_executor.cpp worker/block_store.cpp)
target_link_libraries(nexus_worker_lib PUBLIC nexus_common)

add_executable(worker worker/main.cpp)
target_link_libraries(worker nexus_worker_lib)

# ---- stand-alone ML tool (train / status / predict) ----------------------------
add_executable(nexus_ml tools/nexus_ml.cpp)
target_link_libraries(nexus_ml nexus_ml_lib)

# ---- tests (ctest) ---------------------------------------------------------------
option(NEXUS_BUILD_TESTS "Build the unit tests" ON)
if(NEXUS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
