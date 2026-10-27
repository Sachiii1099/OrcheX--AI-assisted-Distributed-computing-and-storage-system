# Unit tests (no PostgreSQL needed) + database tests (skipped, exit code 77, when
# NEXUS_DB_PASSWORD is not set). Run:  ctest --output-on-failure   (or ./scripts/run_tests.sh)
set(UNIT_TESTS test_message test_scheduler test_worker_exec test_storage test_ml test_http test_affinity)

foreach(t ${UNIT_TESTS})
  add_executable(${t} ${t}.cpp)
  target_link_libraries(${t} nexus_master_lib nexus_worker_lib nexus_ml_lib)
  add_test(NAME ${t} COMMAND ${t})
endforeach()

add_executable(test_db test_db.cpp)
target_link_libraries(test_db nexus_master_lib nexus_worker_lib nexus_ml_lib)
target_compile_definitions(test_db PRIVATE NEXUS_SOURCE_DIR="${CMAKE_SOURCE_DIR}")
add_test(NAME test_db COMMAND test_db)
set_tests_properties(test_db PROPERTIES SKIP_RETURN_CODE 77)
set_tests_properties(test_ml PROPERTIES SKIP_RETURN_CODE 77)

# End-to-end test with real Master + Worker processes (needs python3, PostgreSQL, NEXUS_DB_NAME=*_test)
find_package(Python3 COMPONENTS Interpreter)
if(Python3_FOUND)
  add_test(NAME integration_test COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/integration_test.py)
  set_tests_properties(integration_test PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 900)
endif()
