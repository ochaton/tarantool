#
# A macro to build the bundled libmisc
macro(libmisc_build)
    set(misc_src
        ${PROJECT_SOURCE_DIR}/third_party/PMurHash.c
        ${PROJECT_SOURCE_DIR}/third_party/base64.c
        ${PROJECT_SOURCE_DIR}/third_party/qsort_arg.c
    )

    set(USEARCH_USE_FP16LIB OFF)
    set(USEARCH_BUILD_LIB_C TRUE)
    set(USEARCH_BUILD_TEST_CPP OFF)
    set(USEARCH_BUILD_BENCH_CPP OFF)
    add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/usearch)

    if (NOT HAVE_MEMMEM)
        list(APPEND misc_src
            ${PROJECT_SOURCE_DIR}/third_party/memmem.c
        )
    endif()

    if (NOT HAVE_MEMRCHR)
        list(APPEND misc_src
            ${PROJECT_SOURCE_DIR}/third_party/memrchr.c
        )
    endif()

    if (NOT HAVE_CLOCK_GETTIME)
        list(APPEND misc_src
            ${PROJECT_SOURCE_DIR}/third_party/clock_gettime.c
        )
    endif()

    add_library(misc STATIC ${misc_src})
    set_target_properties(misc PROPERTIES COMPILE_FLAGS "${DEPENDENCY_CFLAGS}")

    unset(misc_src)
endmacro(libmisc_build)
