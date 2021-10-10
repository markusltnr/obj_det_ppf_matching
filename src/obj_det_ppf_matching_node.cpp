#include "obj_det_ppf_matching_node.h"

#include <pcl/console/parse.h>
#include <pcl/io/pcd_io.h>
#include <pcl/io/ply_io.h>

#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <chrono>

#include <PPFRecognizerParameter.h>

using namespace obj_det_ppf_matching_msgs;



bool DetectAndMatchObjectsROS::initialize (int argc, char ** argv)
{
    nh_.reset( new ros::NodeHandle ( "~" ) );
    message_store.reset(new mongodb_store::MessageStoreProxy(*nh_));

    // get all model objects from DB
    std::vector< boost::shared_ptr<Table> > results;
    message_store->query<Table>(results);
    BOOST_FOREACH( boost::shared_ptr<Table> p,  results)
    {
        ROS_INFO_STREAM("Got: " << *p);
    }


}


int main (int argc, char ** argv)
{
    ros::init (argc, argv, "obj_det_and_matching");
    DetectAndMatchObjectsROS m;
    m.initialize (argc, argv);
    ros::spin ();
    return 0;
}

