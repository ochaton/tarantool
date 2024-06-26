#
# A macro to build the third party usearch
macro(libusearch_build)
    set(USEARCH_USE_FP16LIB ON)
    set(USEARCH_BUILD_LIB_C TRUE)
    set(USEARCH_BUILD_TEST_CPP OFF)
    set(USEARCH_BUILD_BENCH_CPP OFF)
    add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/usearch)
endmacro(libusearch_build)
