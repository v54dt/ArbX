cmake_minimum_required(VERSION 3.14)
project(ArbXNotifier CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

include(FetchContent)

FetchContent_Declare(cpp-httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG        v0.18.5)
FetchContent_MakeAvailable(cpp-httplib)

FetchContent_Declare(tomlplusplus
  GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
  GIT_TAG        v3.4.0)
FetchContent_MakeAvailable(tomlplusplus)

FetchContent_Declare(nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG        v3.11.3
  GIT_SHALLOW    TRUE)
FetchContent_MakeAvailable(nlohmann_json)

find_package(CURL REQUIRED)

add_executable(notifier src/main.cpp)
target_link_libraries(notifier PRIVATE
  httplib::httplib
  tomlplusplus::tomlplusplus
  nlohmann_json::nlohmann_json
  CURL::libcurl
  pthread)
