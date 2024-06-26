set(lib_sources
    usearch.cc
)

set_source_files_compile_flags(${lib_sources})

add_library(tt_usearch STATIC ${lib_sources})
target_include_directories(tt_usearch PRIVATE third_party/usearch/include)
