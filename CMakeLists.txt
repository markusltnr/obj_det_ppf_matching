cmake_minimum_required(VERSION 3.0.2)
project(obj_det_ppf_matching)

## Compile as C++11, supported in ROS Kinetic and newer
set (CMAKE_CXX_STANDARD 14)

set(CMAKE_BUILD_TYPE Release)

find_package(OpenCV 3 REQUIRED)


#SET(CMAKE_PREFIX_PATH "/home/edith/Software/pcl-1.10.0/build/install")
#SET(PCL_DIR "/home/edith/Software/pcl-1.10.0/build")
#SET(PCL_INCLUDE_DIRS "/home/edith/Software/pcl-1.10.0/build/install/include/pcl-1.10")
#list(APPEND CMAKE_MODULE_PATH "/home/edith/Software/pcl-1.10.0/cmake/Modules")
#find_package(PCL 1.10 REQUIRED)

## Find catkin macros and libraries
## if COMPONENTS list like find_package(catkin REQUIRED COMPONENTS xyz)
## is used, also find other catkin packages
find_package(catkin REQUIRED COMPONENTS
  mongodb_store
  roscpp
  rospy
  std_msgs
  obj_det_ppf_matching_msgs
  table_extractor
  pcl_conversions
)

## System dependencies are found with CMake's conventions
#list(APPEND CMAKE_MODULE_PATH "/home/edith/Projects/TidyUpVisionPipeline/v4r_ppf/cmake")

add_subdirectory(v4r_ppf) #this only works for folders that are physically a subdirectory. we can not set a path here

find_package( PCL REQUIRED )

find_package(Boost REQUIRED COMPONENTS system program_options)
#find_package(OpenMP)
#find_package(CMPH REQUIRED)
# Glog  -- only works if glog was built with cmake ?
# libgoogle-glog-dev package doesn't include the cmake config files
#find_package(glog 0.4.0 REQUIRED)
#find_library(PPF_RECOGNIZER_LIB NAMES ppf-recognizer PATHS /home/edith/Projects/TidyUpVisionPipeline/build/v4r_ppf)
#find_library(V4R_EXTRACTS_LIB NAMES v4r-extracts PATHS /home/edith/Projects/TidyUpVisionPipeline/build/v4r_ppf)
#find_library(PPF_LIB NAMES ppf PATHS /home/edith/Projects/TidyUpVisionPipeline/build/v4r_ppf)


SET(cd_path "/home/edith/Projects/TidyUpVisionPipeline/change_detection")
#file(GLOB_RECURSE cd_sources ${cd_path}/src/*.cpp)
#message(STATUS ${cd_sources})

## Uncomment this if the package has a setup.py. This macro ensures
## modules and global scripts declared therein get installed
## See http://ros.org/doc/api/catkin/html/user_guide/setup_dot_py.html
# catkin_python_setup()

################################################
## Declare ROS messages, services and actions ##
################################################

## To declare and build messages, services or actions from within this
## package, follow these steps:
## * Let MSG_DEP_SET be the set of packages whose message types you use in
##   your messages/services/actions (e.g. std_msgs, actionlib_msgs, ...).
## * In the file package.xml:
##   * add a build_depend tag for "message_generation"
##   * add a build_depend and a exec_depend tag for each package in MSG_DEP_SET
##   * If MSG_DEP_SET isn't empty the following dependency has been pulled in
##     but can be declared for certainty nonetheless:
##     * add a exec_depend tag for "message_runtime"
## * In this file (CMakeLists.txt):
##   * add "message_generation" and every package in MSG_DEP_SET to
##     find_package(catkin REQUIRED COMPONENTS ...)
##   * add "message_runtime" and every package in MSG_DEP_SET to
##     catkin_package(CATKIN_DEPENDS ...)
##   * uncomment the add_*_files sections below as needed
##     and list every .msg/.srv/.action file to be processed
##   * uncomment the generate_messages entry below
##   * add every package in MSG_DEP_SET to generate_messages(DEPENDENCIES ...)

## Generate messages in the 'msg' folder
# add_message_files(
#   FILES
#   Message1.msg
#   Message2.msg
# )

## Generate services in the 'srv' folder
# add_service_files(
#   FILES
#   Service1.srv
#   Service2.srv
# )

## Generate actions in the 'action' folder
# add_action_files(
#   FILES
#   Action1.action
#   Action2.action
# )

## Generate added messages and services with any dependencies listed here
# generate_messages(
#   DEPENDENCIES
#   std_msgs
# )

################################################
## Declare ROS dynamic reconfigure parameters ##
################################################

## To declare and build dynamic reconfigure parameters within this
## package, follow these steps:
## * In the file package.xml:
##   * add a build_depend and a exec_depend tag for "dynamic_reconfigure"
## * In this file (CMakeLists.txt):
##   * add "dynamic_reconfigure" to
##     find_package(catkin REQUIRED COMPONENTS ...)
##   * uncomment the "generate_dynamic_reconfigure_options" section below
##     and list every .cfg file to be processed

## Generate dynamic reconfigure parameters in the 'cfg' folder
# generate_dynamic_reconfigure_options(
#   cfg/DynReconf1.cfg
#   cfg/DynReconf2.cfg
# )

###################################
## catkin specific configuration ##
###################################
## The catkin_package macro generates cmake config files for your package
## Declare things to be passed to dependent projects
## INCLUDE_DIRS: uncomment this if your package contains header files
## LIBRARIES: libraries you create in this project that dependent projects also need
## CATKIN_DEPENDS: catkin_packages dependent projects also need
## DEPENDS: system dependencies of this project that dependent projects also need
catkin_package(
  INCLUDE_DIRS include
#  LIBRARIES obj_det_ppf_matching
  CATKIN_DEPENDS mongodb_store roscpp rospy std_msgs obj_det_ppf_matching_msgs table_extractor pcl_conversions
#  DEPENDS system_lib
)

###########
## Build ##
###########

add_definitions(${PCL_DEFINITIONS})

## Specify additional locations of header files
## Your package locations should be listed before other locations
include_directories(
  include
  ${PCL_INCLUDE_DIRS}
  ${catkin_INCLUDE_DIRS}
  #"/home/edith/Projects/TidyUpVisionPipeline/v4r_ppf/include/"
  ${cd_path}/include
)

## Declare a C++ library
# add_library(${PROJECT_NAME}
#   src/${PROJECT_NAME}/obj_det_ppf_matching.cpp
# )

## Add cmake target dependencies of the library
## as an example, code may need to be generated before libraries
## either from message generation or dynamic reconfigure
# add_dependencies(${PROJECT_NAME} ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})

## Declare a C++ executable
## With catkin_make all packages are built within a single CMake context
## The recommended prefix ensures that target names across packages don't collide
add_executable(${PROJECT_NAME}_node src/obj_det_ppf_matching_node.cpp
    ${cd_path}/src/change_detection.cpp ${cd_path}/src/color_histogram.cpp ${cd_path}/src/local_object_verification.cpp
    ${cd_path}/src/object_matching.cpp ${cd_path}/src/plane_object_extraction.cpp ${cd_path}/src/scene_differencing_points.cpp)


## Rename C++ executable without prefix
## The above recommended prefix causes long target names, the following renames the
## target back to the shorter version for ease of user use
## e.g. "rosrun someones_pkg node" instead of "rosrun someones_pkg someones_pkg_node"
# set_target_properties(${PROJECT_NAME}_node PROPERTIES OUTPUT_NAME node PREFIX "")

## Add cmake target dependencies of the executable
## same as for the lib${PCL_LIBRARIES}rary above
add_dependencies(${PROJECT_NAME}_node ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})

## Specify libraries to link a library or executable target against
target_link_libraries(${PROJECT_NAME}_node
   ppf-recognizer
   ${catkin_LIBRARIES}
   ${PCL_LIBRARIES}
   ${OpenCV_LIBS}
   Boost::program_options
 )


#############
## Install ##
#############

# all install targets should use catkin DESTINATION variables
# See http://ros.org/doc/api/catkin/html/adv_user_guide/variables.html

## Mark executable scripts (Python etc.) for installation
## in contrast to setup.py, you can choose the destination
# catkin_install_python(PROGRAMS
#   scripts/my_python_script
#   DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
# )

## Mark executables for installation
## See http://docs.ros.org/melodic/api/catkin/html/howto/format1/building_executables.html
# install(TARGETS ${PROJECT_NAME}_node
#   RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
# )

## Mark libraries for installation
## See http://docs.ros.org/melodic/api/catkin/html/howto/format1/building_libraries.html
 install(TARGETS ${PROJECT_NAME}_node
   ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
   LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
   RUNTIME DESTINATION ${CATKIN_GLOBAL_BIN_DESTINATION}
 )

## Mark cpp header files for installation
 install(DIRECTORY include/
   DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
   FILES_MATCHING PATTERN "*.h"
   PATTERN ".svn" EXCLUDE
 )

 install(DIRECTORY launch/ 
   DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch
   FILES_MATCHING PATTERN "*.launch"
 )	

## Mark other files for installation (e.g. launch and bag files, etc.)
# install(FILES
#   # myfile1
#   # myfile2
#   DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
# )

#############
## Testing ##
#############

## Add gtest based cpp test target and link libraries
# catkin_add_gtest(${PROJECT_NAME}-test test/test_obj_det_ppf_matching.cpp)
# if(TARGET ${PROJECT_NAME}-test)
#   target_link_libraries(${PROJECT_NAME}-test ${PROJECT_NAME})
# endif()

## Add folders to be run by python nosetests
# catkin_add_nosetests(test)
