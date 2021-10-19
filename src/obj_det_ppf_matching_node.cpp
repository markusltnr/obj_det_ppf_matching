#include "obj_det_ppf_matching_node.h"

using namespace obj_det_ppf_matching_msgs;

bool DetectAndMatchObjectsROS::extractPermanentObjects (extract_permanent_objects::Request & req,
                                                        extract_permanent_objects::Response & response) {
    ROS_INFO("Service to extract permanent objects was called");

    std::vector<int> interested_tables = req.table_numbers;

    //delete model objects in DB from interested tables
    std::vector<int> del_model_ids = deleteModelObjectsFromDB(interested_tables);

    //delete corresponding model_objects in ppf_model_folder
    std::cout << "Deleted model objects with ID ";
    for (size_t i = 0; i < del_model_ids.size(); i++) {
        std::string orig_path = ppf_model_path_ + "/" + std::to_string(del_model_ids[i]);
        if (boost::filesystem::exists(orig_path)) {
            boost::filesystem::remove_all(orig_path);
            std::cout << del_model_ids[i] << " ";
        }
    }
    std::cout << std::endl;

    int nr_det_objects=0;
    pcl::PointCloud<PointNormal>::Ptr checked_plane_point_cloud(new pcl::PointCloud<PointNormal>);
    std::map<int, Reconstruction> recos = prepareRecos (interested_tables);
    for (std::map<int, Reconstruction>::iterator reco_it=recos.begin(); reco_it!=recos.end(); reco_it++) {
        Reconstruction reco = reco_it->second;
        int table_id = reco_it->first;
        /// downsample reconstruction
        pcl::PointCloud<PointNormal>::Ptr reco_cloud_ds(new pcl::PointCloud<PointNormal>);
        reco_cloud_ds = downsampleCloudVG(reco.reco_cloud, ds_leaf_size_LV);

        std::vector<PlaneWithObjInd> det_objects = cd_->getObjectsFromPlane(reco_cloud_ds, reco.plane_coeffs, reco.cv_hull_points,
                                                                            checked_plane_point_cloud, reco.debug_path);

        /// region growing to get more precise objects
        if (det_objects.size() > 0) {
            cd_->objectRegionGrowing(reco_cloud_ds, det_objects);  //this removes very big clusters after growing
            cd_->mergeObjects(det_objects); //in case an object was detected several times (disjoint sets originally, but prob. overlapping after region growing)
            pcl::PointCloud<PointNormal>::Ptr original_object_cloud = cd_->fromObjectVecToObjectCloud(det_objects, reco_cloud_ds);
            pcl::io::savePCDFileBinary(reco.debug_path + "/result_after_objectGrowing.pcd", *original_object_cloud);

            cd_->upsampleObjectsAndPlanes(reco.reco_cloud, reco_cloud_ds, det_objects, ds_leaf_size_LV, reco.debug_path);
            pcl::PointCloud<PointNormal>::Ptr orig_objects_cloud = cd_->fromObjectVecToObjectCloud(det_objects, reco.reco_cloud);
            pcl::io::savePCDFileBinary(reco.debug_path + "/final_objects.pcd", *orig_objects_cloud);

            std::string debug_model_path = reco.debug_path + "/model_objects/";
            std::vector<DetectedObject> objects_vec;
            for (size_t o = 0; o < det_objects.size(); o++) {
                DetectedObject obj = cd_->fromPlaneIndObjToDetectedObject(reco.reco_cloud, det_objects[o]);
                pcl::PointCloud<PointNormal>::Ptr refined_normals_cloud (new pcl::PointCloud<PointNormal>);
                pcl::copyPointCloud(*(obj.getObjectCloud()), *refined_normals_cloud);
                cd_->refineNormals(refined_normals_cloud);
                obj.setObjectCloud(refined_normals_cloud);
                objects_vec.push_back(obj);
            }
            cd_->filterUnwantedObjects(objects_vec, min_object_volume, min_object_size_ds);

            //save model objects to DB and create folder for PPF
            for (size_t o=0; o < objects_vec.size(); o++) {
                DetectedObject &obj = objects_vec[o];
                std::string debug_obj_folder = debug_model_path + std::to_string(obj.getID()); //PPF uses the folder name as model_id!
                boost::filesystem::create_directories(debug_obj_folder);
                std::string obj_folder = ppf_model_path_ + std::to_string(obj.getID()); //PPF uses the folder name as model_id!
                boost::filesystem::create_directories(obj_folder);
                pcl::io::savePCDFile(debug_obj_folder + "/3D_model.pcd", *(obj.getObjectCloud())); //each detected reference object is saved in a folder for further use with PPF
                pcl::io::savePCDFileBinary(debug_obj_folder + "/plane.pcd", *(obj.plane_cloud_));
                pcl::io::savePCDFile(obj_folder + "/3D_model.pcd", *(obj.getObjectCloud()));
                obj.object_folder_path_ = obj_folder;

                //create the object message
                obj_det_ppf_matching_msgs::Object obj_msg = fromDetObjToMsg(obj, table_id);

                message_store_->insertNamed(std::to_string(obj.getID()), obj_msg);
            }
            nr_det_objects+=objects_vec.size();
        }
    }

    //write the latest used ID to file
    std::ofstream id_file;
    id_file.open (obj_id_path);
    id_file << std::to_string(DetectedObject::getIDCounter());
    id_file.close();

    response.nr_det_obj = nr_det_objects;

    return true;
}

bool DetectAndMatchObjectsROS::detectAndCompareObjects (det_and_compare_obj::Request & req,
                                                        det_and_compare_obj::Response & response) {
    ROS_INFO("Service to extract current objects and compare to model objects in DB was called");

    std::vector<int> interested_tables = req.table_numbers;

    //delete model objects in DB from interested tables
    std::vector<int> del_model_ids = deleteCandidateObjectsFromDB(interested_tables);


    int nr_det_objects=0;
    pcl::PointCloud<PointNormal>::Ptr checked_plane_point_cloud(new pcl::PointCloud<PointNormal>);
    std::map<int, Reconstruction> recos = prepareRecos (interested_tables);
    std::map<int, std::vector<DetectedObject> >  model_objects = getModelObjectsFromDB();

}

std::map<int, Reconstruction> DetectAndMatchObjectsROS::prepareRecos (std::vector<int> interested_tables) {
    std::map<int, Reconstruction> recos;

    /// get all extracted planes from DB
    std::map<int, boost::shared_ptr<table_extractor::Table>> tables = getExtractedTablesFromDB();
    ROS_INFO("extracted tables from DB. Found %ld table", tables.size());

    if (interested_tables.size() == 0) { //get all available table numbers
        for(std::map<int, boost::shared_ptr<table_extractor::Table>>::iterator it = tables.begin(); it != tables.end(); ++it) {
            interested_tables.push_back(it->first);
        }
    }

    /// for each table get reconstruction
    std::map<int, boost::shared_ptr<table_extractor::Table>>::iterator table_it;
    for (table_it = tables.begin(); table_it != tables.end(); table_it++) {
        if (std::find(interested_tables.begin(), interested_tables.end(), table_it->first) == interested_tables.end())
            continue;

        std::string table_path = reco_folder_ + "/scene_" + std::to_string(table_it->first) + ".ply";
        pcl::PointCloud<PointNormal>::Ptr reco_cloud(new pcl::PointCloud<PointNormal>);
        if (!readRecoFromFile(table_path, reco_cloud)) {
            //            response.nr_det_obj = 0;
            //            return false;
            continue;
        }

        /// read plane parameters
        boost::shared_ptr<table_extractor::Table> table = table_it->second;
        Eigen::Vector4f plane_coeffs;
        plane_coeffs << table->plane.x, table->plane.y, table->plane.z, table->plane.d;

        //read transformation from camera to map frame
        Matrix4f_NotAligned transformation = transformationParser(reco_folder_ + "/log.txt");

        //transform the reco
        pcl::transformPointCloudWithNormals(*reco_cloud, *reco_cloud, transformation.inverse());

        /// read convex hull points
        pcl::PointCloud<pcl::PointXYZ>::Ptr convex_hull_pts(new pcl::PointCloud<pcl::PointXYZ>);
        std::vector<geometry_msgs::PointStamped>& ch_points = table->points;
        for (size_t i = 0; i < ch_points.size(); i++) {
            pcl::PointXYZ p;
            p.x = ch_points[i].point.x; p.y = ch_points[i].point.y; p.z = ch_points[i].point.z;
            convex_hull_pts->points.push_back(p);
        }

        std::string debug_path_table = debug_path_permanent_obj_ + "table" + std::to_string(table_it->first);
        if (boost::filesystem::exists(debug_path_table)) {
            boost::filesystem::remove_all(debug_path_table);
        }
        boost::filesystem::create_directories(debug_path_table);

        pcl::io::savePCDFileBinary(debug_path_table + "/cv_hull.pcd", *convex_hull_pts);
        pcl::io::savePCDFileBinary(debug_path_table + "/transformed_input_cloud.pcd", *reco_cloud);

        Reconstruction r(reco_cloud, convex_hull_pts, plane_coeffs, debug_path_table);
        recos[table_it->first] = r;
    }

    return recos;
}

std::map<int, std::vector<DetectedObject> > DetectAndMatchObjectsROS::getModelObjectsFromDB() {
    std::map<int, std::vector<DetectedObject> > model_objects;

    // get all model objects from DB
    std::vector< boost::shared_ptr<obj_det_ppf_matching_msgs::Object> > results;
    message_store_->query<obj_det_ppf_matching_msgs::Object>(results);
    ROS_INFO("Found %ld model objects in DB", results.size());
    BOOST_FOREACH( boost::shared_ptr<obj_det_ppf_matching_msgs::Object> p,  results)
    {
        std::tuple<int, DetectedObject> obj_tuple = fromMsgToDetObj(*p);
        int table_id = std::get<0>(obj_tuple);
        model_objects[table_id].push_back(std::get<1>(obj_tuple));
    }
    return model_objects;
}

//we can only delete objects by ID, which we get at the time of injection to the DB or with queryNamed
std::vector<int> DetectAndMatchObjectsROS::deleteCandidateObjectsFromDB(std::vector<int> table_numbers) {
    std::vector<int> del_model_ids;

    // get all object candidates from DB
    std::vector< boost::shared_ptr<obj_det_ppf_matching_msgs::CandidateObject> > all_candidate_obj_results;
    message_store_->query<obj_det_ppf_matching_msgs::CandidateObject>(all_candidate_obj_results);
    ROS_INFO("Found %ld candidate objects in DB", all_candidate_obj_results.size());

    BOOST_FOREACH( boost::shared_ptr<obj_det_ppf_matching_msgs::CandidateObject> p,  all_candidate_obj_results) {
        if (std::find(table_numbers.begin(), table_numbers.end(), p->object.plane_id) != table_numbers.end()) {
            std::pair< boost::shared_ptr<obj_det_ppf_matching_msgs::CandidateObject>, mongo::BSONObj> result;
            result = message_store_->queryNamed<obj_det_ppf_matching_msgs::CandidateObject>(std::to_string(p->object.id));
            const mongo::BSONObj & meta = (result.second);
            mongo::BSONElement b_elem;
            meta.getObjectID(b_elem);
            std::string id = b_elem.value();
            if (message_store_->deleteID(id)) { //reads the _id field
                del_model_ids.push_back(p->object.id);
            }
        }
    }
    ROS_INFO("Deleted %ld old object candidates from DB", del_model_ids.size());
    return del_model_ids;
}

//we can only delete models by ID, which we get at the time of injection to the DB or with queryNamed
std::vector<int> DetectAndMatchObjectsROS::deleteModelObjectsFromDB(std::vector<int> table_numbers) {
    std::vector<int> del_model_ids;
    std::map<int, std::vector<DetectedObject> > model_objects_map = getModelObjectsFromDB();

    for (size_t i = 0; i < table_numbers.size(); i++) {
        if (model_objects_map.find(table_numbers[i]) != model_objects_map.end()) {
            const std::vector<DetectedObject> & model_objects =  model_objects_map.find(table_numbers[i])->second;
            for (size_t m = 0; m < model_objects.size(); m++) {
                const DetectedObject & m_obj = model_objects[m];

                std::pair< boost::shared_ptr<obj_det_ppf_matching_msgs::Object>, mongo::BSONObj> result;
                result = message_store_->queryNamed<obj_det_ppf_matching_msgs::Object>(std::to_string(m_obj.getID()));

                const mongo::BSONObj & meta = (result.second);
                //std::string id = meta["_id"];
                mongo::BSONElement b_elem;
                meta.getObjectID(b_elem);
                std::string id = b_elem.value();
                if (message_store_->deleteID(id)) { //reads the _id field
                    del_model_ids.push_back(m_obj.getID());
                }
            }
        }
    }
    ROS_INFO("Deleted %ld old object models from DB", del_model_ids.size());
    return del_model_ids;
}

std::map<int, boost::shared_ptr<table_extractor::Table>> DetectAndMatchObjectsROS::getExtractedTablesFromDB() {
    std::map<int, boost::shared_ptr<table_extractor::Table>> extracted_tables;

    // get all tables from DB
    std::vector< boost::shared_ptr<table_extractor::Table> > results;
    message_store_->query<table_extractor::Table>(results);
    BOOST_FOREACH( boost::shared_ptr<table_extractor::Table> p,  results)
    {
        extracted_tables[p->id] = p;
    }
    return extracted_tables;
}

std::tuple<int, DetectedObject> DetectAndMatchObjectsROS::fromMsgToDetObj(obj_det_ppf_matching_msgs::Object obj_msg) {
    pcl::PointCloud<PointNormal>::Ptr obj_cloud(new pcl::PointCloud<PointNormal>);
    pcl::fromROSMsg(obj_msg.obj_cloud, *obj_cloud);

    pcl::PointCloud<PointNormal>::Ptr plane_cloud(new pcl::PointCloud<PointNormal>);
    pcl::fromROSMsg(obj_msg.plane_cloud, *plane_cloud);

    DetectedObject obj(obj_msg.id, obj_cloud, plane_cloud, ObjectState::UNKNOWN, obj_msg.object_path);

    int table_id = obj_msg.plane_id;

    return std::make_tuple(table_id, obj);
}

obj_det_ppf_matching_msgs::Object DetectAndMatchObjectsROS::fromDetObjToMsg(DetectedObject obj, int table_id) {
    obj_det_ppf_matching_msgs::Object obj_msg;

    obj_msg.id = obj.getID();
    obj_msg.plane_id = table_id;
    sensor_msgs::PointCloud2 obj_cloud_msg;
    pcl::toROSMsg(*obj.getObjectCloud(), obj_cloud_msg);
    obj_msg.obj_cloud = obj_cloud_msg;
    sensor_msgs::PointCloud2 plane_cloud_msg;
    pcl::toROSMsg(*(obj.plane_cloud_), plane_cloud_msg);
    obj_msg.plane_cloud = plane_cloud_msg;
    obj_msg.object_path = obj.object_folder_path_.c_str();

    return obj_msg;
}

bool DetectAndMatchObjectsROS::readRecoFromFile(std::string input_path, const pcl::PointCloud<PointNormal>::Ptr &cloud) {
    std::string ext = input_path.substr(input_path.length()-3, input_path.length());
    if (ext == "ply") {
        if (pcl::io::loadPLYFile<PointNormal> (input_path, *cloud) == -1)
        {
            std::cerr << "Couldn't read file " << input_path << std::endl;
            return false;
        }
        return true;
    } else if (ext == "pcd") {
        if (pcl::io::loadPCDFile<PointNormal> (input_path, *cloud) == -1)
        {
            std::cerr << "Couldn't read file " << input_path << std::endl;
            return false;
        }
        return true;
    } else {
        std::cerr << "Couldn't read file " << input_path << ". It needs to be a ply or pcd file" << std::endl;
        return false;
    }
}


//assuming there is one transformation stored in the file (different to parser used for the IROS dataset)
Matrix4f_NotAligned DetectAndMatchObjectsROS::transformationParser(std::string path) {
    std::ifstream file(path);
    if (!file.good()) {
        std::cerr << "File " << path << "does not exist << std::endl";
        return Matrix4f_NotAligned::Identity();
    }
    std::string str;
    //Table 1, start at 1567875713.42, occurance 0. Transform:
    std::getline(file, str);
    if (str.rfind("Table",0) == 0) {
        std::vector<std::string> split_result;

        //now extract the transformation matrix
        Matrix4f_NotAligned mat;
        std::getline(file, str);
        boost::split(split_result, str, boost::is_any_of("[[ ]"), boost::token_compress_on);
        mat(0,0) = std::stof(split_result[2]);
        mat(0,1) = std::stof(split_result[3]);
        mat(0,2) = std::stof(split_result[4]);
        mat(0,3) = std::stof(split_result[5]);

        std::getline(file, str);
        str = str.substr(2, str.size() - 3); //remove the first 2 chars and the last one
        boost::trim(str);
        std::vector<std::string> transf_split_result;
        boost::split(transf_split_result, str, boost::is_any_of(" "), boost::token_compress_on);
        mat(1,0) = std::stof(transf_split_result[0]);
        mat(1,1) = std::stof(transf_split_result[1]);
        mat(1,2) = std::stof(transf_split_result[2]);
        mat(1,3) = std::stof(transf_split_result[3]);

        std::getline(file, str);
        str = str.substr(2, str.size() - 3); //remove the first 2 chars and the last one
        boost::trim(str);
        boost::split(transf_split_result, str, boost::is_any_of(" "), boost::token_compress_on);
        mat(2,0) = std::stof(transf_split_result[0]);
        mat(2,1) = std::stof(transf_split_result[1]);
        mat(2,2) = std::stof(transf_split_result[2]);
        mat(2,3) = std::stof(transf_split_result[3]);

        std::getline(file, str);
        str = str.substr(2, str.size() - 4); //remove the first 2 chars and the last two
        boost::trim(str);
        boost::split(transf_split_result, str, boost::is_any_of(" "), boost::token_compress_on);
        mat(3,0) = std::stof(transf_split_result[0]);
        mat(3,1) = std::stof(transf_split_result[1]);
        mat(3,2) = std::stof(transf_split_result[2]);
        mat(3,3) = std::stof(transf_split_result[3]);

        return mat;
    }
    return Matrix4f_NotAligned::Identity();
}

bool DetectAndMatchObjectsROS::initialize (int argc, char ** argv)
{
    nh_.reset( new ros::NodeHandle ( "~" ) );
    message_store_.reset(new mongodb_store::MessageStoreProxy(*nh_));

    if (!nh_->getParam("reco_folder", reco_folder_)) {
        ROS_ERROR("Reconstruction directory is not set. Must be set with ROS parameter \"reco_folder\"!");
        return false;
    }
    if (!nh_->getParam("ppf_config", ppf_config_path_)) {
        ROS_ERROR("Path to ppf config file is not set. Must be set with ROS parameter \"ppf_config\"!");
        return false;
    }
    //optional, otherwise it will be stored in the devel-folder
    nh_->getParam("ppf_model_folder", ppf_model_path_);
    nh_->getParam("debug_path_permanent_obj", debug_path_permanent_obj_);

    if (debug_path_permanent_obj_ != "")
        debug_path_permanent_obj_ += "/";
    if (ppf_model_path_  != "")
        ppf_model_path_ += "/";

    extract_perm_objs_  = nh_->advertiseService ("extract_perm_objects", &DetectAndMatchObjectsROS::extractPermanentObjects, this);
    det_and_compare_objects_  = nh_->advertiseService ("det_and_compare_objects", &DetectAndMatchObjectsROS::detectAndCompareObjects, this);

    //Create ChangeDetection object
    cd_.reset(new ChangeDetection (ppf_config_path_));
    ppf_model_path_ += "model_objects/";
    if (!boost::filesystem::exists((ppf_model_path_)))
        boost::filesystem::create_directories(ppf_model_path_);
    cd_->setPPFModelPath(ppf_model_path_);
    ROS_INFO("Created ChangeDetection object");

    //read this ID if the file exists
    if (boost::filesystem::exists(obj_id_path)) {
        std::ifstream id_file;
        id_file.open (obj_id_path);
        std::string line;
        std::getline (id_file,line);
        int latest_id = std::atoi(line.c_str());
        DetectedObject::setIDCounter(latest_id);
        id_file.close();
    }

    ROS_INFO("Ready to get service calls.");
    std::cout << "ready" << std::endl;

    return true;

}

int main (int argc, char ** argv)
{
    ros::init (argc, argv, "obj_det_and_matching");
    DetectAndMatchObjectsROS m;
    m.initialize (argc, argv);
    ros::spin ();
    return 0;
}

