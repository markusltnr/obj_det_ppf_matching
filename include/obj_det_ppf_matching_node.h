#include "ros/ros.h"
#include "mongodb_store/message_store.h"

#include "obj_det_ppf_matching_msgs/Table.h"
#include "obj_det_ppf_matching_msgs/det_and_compare_obj.h"
#include "obj_det_ppf_matching_msgs/DetectedObject.h"



class DetectAndMatchObjectsROS
{
private:
    boost::shared_ptr<ros::NodeHandle> nh_;
    boost::shared_ptr<mongodb_store::MessageStoreProxy> message_store;
//boost::shared_ptr<ChangeDetection> cd_; ///< change detection and matching

    //    boost::shared_ptr<image_transport::ImageTransport> it_;
    //    image_transport::Publisher image_pub_;

    //    ros::Publisher vis_pc_pub_;
    //    ros::ServiceServer recognize_, recognizer_set_camera_;

    //    std::vector< ObjectHypothesesGroup > object_hypotheses_; ///< recognized objects

    //    typename pcl::PointCloud<PointT>::Ptr scene_; ///< input cloud
    //    Camera::ConstPtr camera_; ///< camera (if cloud is not organized)

    //    bool respondSrvCall(v4r_object_recognition_msgs::recognize::Request &req, v4r_object_recognition_msgs::recognize::Response &response) const;

public:
    DetectAndMatchObjectsROS()    {}

    /* bool recognizeROS (v4r_object_recognition_msgs::recognize::Request & req,
                       v4r_object_recognition_msgs::recognize::Response & response);

    bool setCamera (v4r_object_recognition_msgs::set_camera::Request & req,
                       v4r_object_recognition_msgs::set_camera::Response & response); */

    bool initialize (int argc, char ** argv);
};



