include(FetchContent)

FetchContent_Declare(
    erofs-utils
    GIT_REPOSITORY https://gitee.com/salvete/erofs-utils.git
    GIT_TAG        overlaybd-dev-rebased-api-3
)

FetchContent_MakeAvailable(erofs-utils)

set(BUILD_DIR ${erofs-utils_SOURCE_DIR})
file(MAKE_DIRECTORY ${BUILD_DIR})

execute_process(
    COMMAND ./autogen.sh
    WORKING_DIRECTORY ${erofs-utils_SOURCE_DIR}
)
execute_process(
    COMMAND ./configure 
    WORKING_DIRECTORY ${BUILD_DIR}
)
execute_process(
    COMMAND make
    WORKING_DIRECTORY ${BUILD_DIR}
)

set(EROFS_LIB_INCLUDE_DIR "${BUILD_DIR}/include/" CACHE PATH "erofs-utils include path.")
set(EROFS_LIB_STATIC  "${EROFS_LIB_DIR}/.libs/liberofs.a" CACHE PATH "erofs-utils static lib.")
set(EROFS_CONFIG_FILE "${BUILD_DIR}/config.h" CACHE PATH "erofs-utils config file.")

file(GLOB EROFS_SOURCE "*.cpp")

add_library(erofs_lib STATIC ${EROFS_SOURCE})

target_include_directories(erofs_lib PRIVATE
    ${PHOTON_INCLUDE_DIR}
)

target_include_directories(erofs_lib PRIVATE
    ${EROFS_LIB_INCLUDE_DIR}
)

target_link_libraries(erofs_lib PRIVATE uuid)
target_link_libraries(erofs_lib PRIVATE lz4)

target_compile_options(erofs_lib PRIVATE "-include${EROFS_CONFIG_FILE}")
target_link_libraries(erofs_lib PRIVATE ${EROFS_LIB_STATIC})