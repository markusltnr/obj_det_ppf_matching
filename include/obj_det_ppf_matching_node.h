#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <chrono>

#include "ros/ros.h"
#include "mongodb_store/message_store.h"
#include <pcl_conversions/pcl_conversions.h>

#include <pcl/console/parse.h>
#include <pcl/io/pcd_io.h>
#include <pcl/io/ply_io.h>

#include "table_extractor/Table.h"
#include "obj_det_ppf_matching_msgs/extract_permanent_objects.h"
#include "obj_det_ppf_matching_msgs/det_and_compare_obj.h"
#include "obj_det_ppf_matching_msgs/CandidateObject.h"
#include "obj_det_ppf_matching_msgs/Object.h"
#include "obj_det_ppf_matching_msgs/CandidateObject.h"

#include <detected_object.h>
#include <mathhelpers.h>
#include <settings.h>
#include <change_detection.h>

using namespace obj_det_ppf_matching_msgs;

typedef pcl::PointXYZRGBNormal PointNormal;

typedef Eigen::Matrix<float,4,4,Eigen::DontAlign> Matrix4f_NotAligned;
typedef Eigen::Matrix<float,4,1,Eigen::DontAlign> Vector4f_NotAligned;

static const std::string obj_id_path = "next_obj_id.txt"; //we store the latest used object ID in a file

struct Reconstruction {
    pcl::PointCloud<PointNormal>::Ptr reco_cloud;
    pcl::PointCloud<pcl::PointXYZ>::Ptr cv_hull_points;
    Vector4f_NotAligned plane_coeffs;
    std::string debug_path;

    Reconstruction() {}
    Reconstruction(pcl::PointCloud<PointNormal>::Ptr cloud, pcl::PointCloud<pcl::PointXYZ>::Ptr cv_hull,
                   Vector4f_NotAligned plane, std::string path) :
        reco_cloud{cloud}, cv_hull_points{cv_hull}, plane_coeffs{plane}, debug_path{path} {}
};

class DetectAndMatchObjectsROS
{
private:
    boost::shared_ptr<ros::NodeHandle> nh_;
    boost::shared_ptr<mongodb_store::MessageStoreProxy> message_store_;
    boost::shared_ptr<ChangeDetection> cd_; ///< change detection and matching

    ros::ServiceServer extract_perm_objs_;
    ros::ServiceServer det_and_compare_objects_;

    std::map<int, boost::shared_ptr<table_extractor::Table>> extracted_tables;
    std::string reco_folder_;
    std::string ppf_model_path_;
    std::string ppf_config_path_;
    std::string debug_path_permanent_obj_;


    bool readRecoFromFile(std::string input_path, const pcl::PointCloud<PointNormal>::Ptr &cloud);
    Matrix4f_NotAligned transformationParser(std::string path);
    std::vector<int> deleteModelObjectsFromDB(std::vector<int> table_numbers);
    std::vector<int> deleteCandidateObjectsFromDB(std::vector<int> table_numbers);
    std::map<int, Reconstruction> prepareRecos (std::vector<int> interested_tables);

    std::tuple<int, DetectedObject> fromMsgToDetObj(obj_det_ppf_matching_msgs::Object obj_msg); //returns table_id and object
    obj_det_ppf_matching_msgs::Object fromDetObjToMsg(DetectedObject obj, int table_id);


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

    bool extractPermanentObjects (extract_permanent_objects::Request & req,
                                  extract_permanent_objects::Response & response);

    bool detectAndCompareObjects (det_and_compare_obj::Request & req,
                                  det_and_compare_obj::Response & response);

    bool initialize (int argc, char ** argv);
    std::map<int, boost::shared_ptr<table_extractor::Table>> getExtractedTablesFromDB();
    std::map<int, std::vector<DetectedObject> > getModelObjectsFromDB();
};



