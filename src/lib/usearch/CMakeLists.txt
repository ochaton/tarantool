
set(lib_sources
    usearch.cc
)

set_source_files_compile_flags(${lib_sources})

include_directories(third_party/usearch/include)
add_library(tt_usearch STATIC ${lib_sources})
target_link_libraries(tt_usearch usearch_c)

