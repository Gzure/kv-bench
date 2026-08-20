cmake_minimum_required(VERSION 3.16)
project(kv_bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

# SDK 有 urma_bondp.h 且支持 bonding balance-mode user_ctl 时开启
# （代码已用 __has_include 自动探测；此开关用于 SDK 头缺失定义时手动强制）
option(KV_BENCH_BONDP_CTL "Force-enable bonding balance-mode user_ctl (BONDP_USER_CTL_BONDING)" OFF)

# ---- URMA SDK 头文件（urma_api.h / urma_ubagg.h），URMA_ROOT 或系统路径 ----
find_path(URMA_INCLUDE_PATH
	NAMES
	urma_api.h
	HINTS
	${URMA_ROOT}
	ENV
	URMA_ROOT
	PATHS
	/usr/include
	/usr/local/include
	PATH_SUFFIXES
	ub/umdk/urma
	umdk/urma
	urma
	src/urma/lib/urma/core/include
)
find_path(URMA_BOND_INCLUDE_PATH
	NAMES
	urma_ubagg.h
	HINTS
	${URMA_ROOT}
	ENV
	URMA_ROOT
	PATHS
	/usr/include
	/usr/local/include
	PATH_SUFFIXES
	ub/umdk/urma
	umdk/urma
	urma
	src/urma/lib/urma/bond/include
)
if(NOT URMA_INCLUDE_PATH OR NOT URMA_BOND_INCLUDE_PATH)
	message(FATAL_ERROR
		"Fail to find URMA headers. Install UMDK headers or set URMA_ROOT."
	)
endif()
message(STATUS "Found URMA headers: ${URMA_INCLUDE_PATH}")
message(STATUS "Found URMA bonding headers: ${URMA_BOND_INCLUDE_PATH}")

# ---- liburma（显式定位，含 /usr/lib64/urma 等非默认路径） ----
find_library(URMA_LIBRARY
	NAMES
	urma
	HINTS
	${URMA_ROOT}
	ENV
	URMA_ROOT
	PATHS
	/usr/lib64/urma
	/usr/lib
	/usr/lib64
	/usr/local/lib
)
if(NOT URMA_LIBRARY)
	message(FATAL_ERROR
		"Fail to find liburma. Install liburma or set URMA_ROOT."
	)
endif()
message(STATUS "Found liburma: ${URMA_LIBRARY}")

add_executable(kv-bench
	src/kv_bench.cpp
	src/urma/urma_resource.cpp
	src/urma/urma_manager.cpp
)
target_include_directories(kv-bench
	PRIVATE
		"${URMA_INCLUDE_PATH}"
		"${URMA_BOND_INCLUDE_PATH}"
		"${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(kv-bench PRIVATE "${URMA_LIBRARY}" pthread)
target_compile_definitions(kv-bench
	PRIVATE
		_GNU_SOURCE
		$<$<BOOL:${KV_BENCH_BONDP_CTL}>:BONDP_USER_CTL_BONDING>
)
