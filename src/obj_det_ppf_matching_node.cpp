#include "obj_det_ppf_matching_node.h"

using namespace obj_det_ppf_matching_msgs;

bool DetectAndMatchObjectsROS::extractPermanentObjects (extract_permanent_objects::Request & req,
                                                        extract_permanent_objects::Response & response) {
    ROS_INFO("Service to extract permanent objects was called");

    std::vector<int> interested_tables = req.table_numbers;

    //delete model objects in DB from interested tables
    std::vector<int> del_model_ids = deleteModelObjectsFromDB(interested_tables);

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

            //save model objects to DB
            for (size_t o=0; o < objects_vec.size(); o++) {
                DetectedObject &obj = objects_vec[o];
                std::string debug_obj_folder = debug_model_path + std::to_string(obj.getID()); //PPF uses the folder name as model_id!
                boost::filesystem::create_directories(debug_obj_folder);
                pcl::io::savePCDFile(debug_obj_folder + "/3D_model.pcd", *(obj.getObjectCloud())); //each detected reference object is saved in a folder for further use with PPF
                pcl::io::savePCDFileBinary(debug_obj_folder + "/plane.pcd", *(obj.plane_cloud_));

                //create the object message
                obj_det_ppf_matching_msgs::Object obj_msg = fromDetObjToObjMsg(obj, table_id);

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

    ROS_INFO("Finished service call");

    return true;
}

std::vector<DetectedObject> fromMapToValVec(std::map<int, DetectedObject> map) {
    //transform map into vec to be able to call object matching
    std::vector<DetectedObject> vec;
    vec.reserve(map.size());
    for(auto const& imap: map)
        vec.push_back(imap.second);
    return vec;
}

bool DetectAndMatchObjectsROS::detectAndCompareObjects (det_and_compare_obj::Request & req,
                                                        det_and_compare_obj::Response & response) {
    ROS_INFO("Service to extract current objects and compare to model objects in DB was called");
    pot_removed_obj.clear();
    pot_new_obj.clear();
    new_obj.clear();
    removed_obj.clear();;
    ref_displaced_obj.clear();
    ref_static_obj.clear();
    curr_displaced_obj.clear();
    curr_static_obj.clear();

    std::vector<int> interested_tables = req.table_numbers;

    //delete candidate objects in DB from interested tables
    std::vector<int> del_model_ids = deleteCandidateObjectsFromDB(interested_tables);

    //get model objects from DB
    std::map<int, std::vector<DetectedObject> >  model_objects = getModelObjectsFromDB(); //table_id, DetectedObject
    createPPFModelFolders(model_objects);

    pcl::PointCloud<PointNormal>::Ptr orig_model_obj_cloud(new pcl::PointCloud<PointNormal>);
    pcl::PointCloud<pcl::PointXYZL>::Ptr table_id_model_cloud(new pcl::PointCloud<pcl::PointXYZL>); //used to find out the table id of splitted objects
    for (std::map<int, std::vector<DetectedObject> >::iterator m_it = model_objects.begin(); m_it!=model_objects.end(); m_it++) {
        const std::vector<DetectedObject> &det_obj_vec = m_it->second;
        for (DetectedObject o : det_obj_vec) {
            *orig_model_obj_cloud += *(o.getObjectCloud());
            for (const PointNormal &p : o.getObjectCloud()->points) {
                pcl::PointXYZL p_id;
                p_id.x = p.x; p_id.y = p.y; p_id.z = p.z;
                p_id.label = m_it->first;
                table_id_model_cloud->points.push_back(p_id);
            }
        }
    }

    int nr_det_objects=0;
    pcl::PointCloud<PointNormal>::Ptr checked_plane_point_cloud(new pcl::PointCloud<PointNormal>);
    std::map<int, Reconstruction> recos = prepareRecos (interested_tables);
    std::map<int, std::vector<DetectedObject> > table_object_map;
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

            for (size_t o = 0; o < det_objects.size(); o++) {
                DetectedObject obj = cd_->fromPlaneIndObjToDetectedObject(reco.reco_cloud, det_objects[o]);
                pcl::PointCloud<PointNormal>::Ptr refined_normals_cloud (new pcl::PointCloud<PointNormal>);
                pcl::copyPointCloud(*(obj.getObjectCloud()), *refined_normals_cloud);
                cd_->refineNormals(refined_normals_cloud);
                obj.setObjectCloud(refined_normals_cloud);
                table_object_map[table_id].push_back(obj);
                nr_det_objects++;

                pcl::io::savePCDFileBinary(reco.debug_path + "/"+ std::to_string(obj.getID()) +".pcd", *obj.getObjectCloud());
            }
        }
    }
    ROS_INFO("Detected %d objects from planes", nr_det_objects);

    pcl::PointCloud<PointNormal>::Ptr orig_candidate_obj_cloud(new pcl::PointCloud<PointNormal>);
    pcl::PointCloud<pcl::PointXYZL>::Ptr table_id_object_cloud(new pcl::PointCloud<pcl::PointXYZL>); //used to find out the table id of splitted objects
    for (std::map<int, std::vector<DetectedObject> >::iterator o_it = table_object_map.begin(); o_it!=table_object_map.end(); o_it++) {
        const std::vector<DetectedObject> &det_obj_vec = o_it->second;
        for (DetectedObject o : det_obj_vec) {
            *orig_candidate_obj_cloud += *(o.getObjectCloud());
            for (const PointNormal &p : o.getObjectCloud()->points) {
                pcl::PointXYZL p_id;
                p_id.x = p.x; p_id.y = p.y; p_id.z = p.z;
                p_id.label = o_it->first;
                table_id_object_cloud->points.push_back(p_id);
            }
        }
    }

    std::string merge_object_parts_folder = debug_path_det_obj_ + "/mergeObjectParts/";
    boost::filesystem::create_directory(merge_object_parts_folder);

    std::map<int, int> obj_table_id;

    //for models and candidate objects from same table
    for (std::map<int, std::vector<DetectedObject> >::iterator ref_it = model_objects.begin(); ref_it != model_objects.end(); ref_it++) {
        std::vector<DetectedObject> ref_result, curr_result;
        if (table_object_map.find(ref_it->first) != table_object_map.end()) {
            int table_id = ref_it->first;

            /// LOCAL VERIFICATION
            std::vector<DetectedObject> &ref_obj_vec = ref_it->second;
            std::vector<DetectedObject> &curr_obj_vec = table_object_map.find(ref_it->first)->second;
            cd_->performLV(ref_obj_vec, curr_obj_vec);

            //upsample and filter current objects
            if (curr_obj_vec.size() > 0) {
                //after LV we need to upsample the objects again
                upsampleAndRefineObjects(orig_candidate_obj_cloud, curr_obj_vec);
                pcl::PointCloud<PointNormal>::Ptr novel_objects_cloud = cd_->fromDetObjectVecToCloud(curr_obj_vec, false);
                if (!novel_objects_cloud->empty())
                    pcl::io::savePCDFileBinary(debug_path_det_obj_ + "/curr_upsampled_objects_after_LV.pcd", *novel_objects_cloud);

                cd_->filterUnwantedObjects(curr_obj_vec, min_object_volume, min_object_size_ds);
                novel_objects_cloud = cd_->fromDetObjectVecToCloud(curr_obj_vec, false);
                if (!novel_objects_cloud->empty())
                    pcl::io::savePCDFileBinary(debug_path_det_obj_ + "/curr_final_objects_after_LV.pcd", *novel_objects_cloud);
            }

            //upsample and filter reference objects
            if (ref_obj_vec.size() > 0) {
                //after LV we need to upsample the objects again
                upsampleAndRefineObjects(orig_model_obj_cloud, ref_obj_vec);
                pcl::PointCloud<PointNormal>::Ptr disappeared_objects_cloud = cd_->fromDetObjectVecToCloud(ref_obj_vec, false);
                if (!disappeared_objects_cloud->empty())
                    pcl::io::savePCDFileBinary(debug_path_det_obj_ + "/ref_upsampled_objects_after_LV.pcd", *disappeared_objects_cloud);

                cd_->filterUnwantedObjects(ref_obj_vec, min_object_volume, min_object_size_ds);
                disappeared_objects_cloud = cd_->fromDetObjectVecToCloud(ref_obj_vec, false);
                if (!disappeared_objects_cloud->empty())
                    pcl::io::savePCDFileBinary(debug_path_det_obj_ + "/ref_final_objects_after_LV.pcd", *disappeared_objects_cloud);
            }

            if (curr_obj_vec.size() == 0) { //all objects removed from ref scene
                for (size_t i = 0; i < ref_obj_vec.size(); i++) {
                    ref_obj_vec[i].state_ = ObjectState::REMOVED;
                }
                ref_result = ref_obj_vec;
                cd_->mergeObjectParts(ref_result, merge_object_parts_folder);
            }

            else if (ref_obj_vec.size() == 0) { //all objects are new in curr
                for (size_t i = 0; i < curr_obj_vec.size(); i++) {
                    curr_obj_vec[i].state_ = ObjectState::NEW;
                }
                curr_result = curr_obj_vec;
                cd_->mergeObjectParts(curr_result, merge_object_parts_folder);
            }

            else {
                //matches between same plane different timestamps
                ObjectMatching object_matching(ref_obj_vec, curr_obj_vec, ppf_model_path_, ppf_config_path_);
                std::vector<Match> matches = object_matching.compute(ref_result, curr_result);

                //region growing of static/displaced objects (should create more precise results if e.g. the model was smaller than die object or not precisely aligned
                cd_->mergeObjectParts(ref_result, merge_object_parts_folder);
                cd_->mergeObjectParts(curr_result, merge_object_parts_folder);
                cd_->filterUnwantedObjects(ref_result, min_object_volume, min_object_size_ds);
                cd_->filterUnwantedObjects(curr_result, min_object_volume, min_object_size_ds);
            }

            updateDetectedObjects(ref_result, curr_result);

            for (DetectedObject o : ref_result)
                obj_table_id[o.getID()] = table_id;
            for (DetectedObject o : curr_result)
                obj_table_id[o.getID()] = table_id;
        }
        else {
            //no matching current table --> add model objects to potential removed objects
            const std::vector<DetectedObject> &obj_vec = ref_it->second;
            for (DetectedObject o : obj_vec) {
                ref_result.push_back(o);
            }
            updateDetectedObjects(ref_result, curr_result);
            for (DetectedObject o : ref_result)
                obj_table_id[o.getID()] = ref_it->first;
        }
    }

    //now add candidate objects from tables that were not matched
    for (std::map<int, std::vector<DetectedObject> >::iterator it = table_object_map.begin(); it != table_object_map.end(); it++) {
        std::vector<DetectedObject> ref_result, curr_result;
        if (model_objects.find(it->first) == model_objects.end()) {
            const std::vector<DetectedObject> &obj_vec = it->second;
            for (DetectedObject o : obj_vec) {
                curr_result.push_back(o);
            }
            updateDetectedObjects(ref_result, curr_result);
            for (DetectedObject o : curr_result)
                obj_table_id[o.getID()] = it->first;
        }
    }

    //after collecting potential new and removed objects from the plane, try to match them
    if (pot_removed_obj.size() != 0 && pot_new_obj.size() != 0) {
        std::string merge_object_parts_folder = debug_path_det_obj_ + "/leftover_mergeObjectParts";
        boost::filesystem::create_directory(merge_object_parts_folder);

        bool newObjectOrModel = true;
        while(newObjectOrModel) {
            //transform map into vec to be able to call object matching
            std::vector<DetectedObject> pot_rem_obj_vec, pot_new_obj_vec;
            pot_rem_obj_vec = fromMapToValVec(pot_removed_obj);
            pot_new_obj_vec = fromMapToValVec(pot_new_obj);
            ObjectMatching matching(pot_rem_obj_vec, pot_new_obj_vec, ppf_model_path_, ppf_config_path_);
            std::vector<DetectedObject> ref_result, curr_result;
            matching.compute(ref_result, curr_result);

            ChangeDetection::mergeObjectParts(ref_result, merge_object_parts_folder);
            ChangeDetection::mergeObjectParts(curr_result, merge_object_parts_folder);

            newObjectOrModel = updateDetectedObjects(ref_result, curr_result);

            //find table id if it does not exist yet
            for (const DetectedObject &o : ref_result) {
                if (obj_table_id.find(o.getID()) == obj_table_id.end()) {
                    int table_id = extractTableID(table_id_model_cloud, o);
                    obj_table_id[o.getID()] = table_id;
                }
            }
            for (const DetectedObject &o : curr_result) {
                if (obj_table_id.find(o.getID()) == obj_table_id.end()) {
                    int table_id = extractTableID(table_id_object_cloud, o);
                    obj_table_id[o.getID()] = table_id;
                }
            }
        }
    }

    removed_obj = pot_removed_obj;
    new_obj = pot_new_obj;

    int highest_model_id = 0;

    //update models in DB
    deleteModelObjectsFromDB(interested_tables);
    std::map<int, DetectedObject>::iterator obj_map_it;
    for (obj_map_it = removed_obj.begin(); obj_map_it != removed_obj.end(); obj_map_it++) {
        const DetectedObject &obj = obj_map_it->second;
        int table_id = obj_table_id[obj.getID()];
        //create the object message
        obj_det_ppf_matching_msgs::Object obj_msg = fromDetObjToObjMsg(obj, table_id);

        message_store_->insertNamed(std::to_string(obj.getID()), obj_msg);

        if (highest_model_id < obj.getID())
            highest_model_id = obj.getID();
    }
    for (obj_map_it = ref_displaced_obj.begin(); obj_map_it != ref_displaced_obj.end(); obj_map_it++) {
        DetectedObject obj = obj_map_it->second;
        int table_id = obj_table_id[obj.getID()];
        //create the object message
        obj_det_ppf_matching_msgs::Object obj_msg = fromDetObjToObjMsg(obj, table_id);

        message_store_->insertNamed(std::to_string(obj.getID()), obj_msg);

        if (highest_model_id < obj.getID())
            highest_model_id = obj.getID();
    }
    for (obj_map_it = ref_static_obj.begin(); obj_map_it != ref_static_obj.end(); obj_map_it++) {
        DetectedObject &obj = obj_map_it->second;
        int table_id = obj_table_id[obj.getID()];
        //create the object message
        obj_det_ppf_matching_msgs::Object obj_msg = fromDetObjToObjMsg(obj, table_id);

        message_store_->insertNamed(std::to_string(obj.getID()), obj_msg);

        if (highest_model_id < obj.getID())
            highest_model_id = obj.getID();
    }

    //insert candidate objects into DB
    for (obj_map_it = new_obj.begin(); obj_map_it != new_obj.end(); obj_map_it++) {
        const DetectedObject &obj = obj_map_it->second;
        int table_id = obj_table_id[obj.getID()];
        //create the object message
        obj_det_ppf_matching_msgs::CandidateObject obj_msg = fromDetObjToCandidateObjMsg(obj, table_id);

        message_store_->insertNamed(std::to_string(obj.getID()), obj_msg);
    }
    for (obj_map_it = curr_static_obj.begin(); obj_map_it != curr_static_obj.end(); obj_map_it++) {
        const DetectedObject &obj = obj_map_it->second;
        int table_id = obj_table_id[obj.getID()];
        //create the object message
        obj_det_ppf_matching_msgs::CandidateObject obj_msg = fromDetObjToCandidateObjMsg(obj, table_id);

        message_store_->insertNamed(std::to_string(obj.getID()), obj_msg);
    }
    for (obj_map_it = curr_displaced_obj.begin(); obj_map_it != curr_displaced_obj.end(); obj_map_it++) {
        const DetectedObject &obj = obj_map_it->second;
        int table_id = obj_table_id[obj.getID()];
        //create the object message
        obj_det_ppf_matching_msgs::CandidateObject obj_msg = fromDetObjToCandidateObjMsg(obj, table_id);

        message_store_->insertNamed(std::to_string(obj.getID()), obj_msg);
    }

    std::cout << "Removed objects: " << removed_obj << std::endl;
    std::cout << "New objects: " << new_obj << std::endl;
    std::cout << "Displaced objects in reference: " << ref_displaced_obj << std::endl;
    std::cout << "Displaced objects in current: " << curr_displaced_obj << std::endl;
    std::cout << "Static objects in reference: " << ref_static_obj << std::endl;
    std::cout << "Static objects in current: " << curr_static_obj << std::endl;


    //write the latest used ID to file --> find the highest object ID for models (candidate objects get deleted anyway for each run)
    std::ofstream id_file;
    id_file.open (obj_id_path);
    id_file << std::to_string(highest_model_id);
    id_file.close();

    ROS_INFO("Finished service call");
}

int DetectAndMatchObjectsROS::extractTableID(const pcl::PointCloud<pcl::PointXYZL>::Ptr label_cloud, const DetectedObject &obj) {
    std::vector<int> nn_indices;
    std::vector<float> nn_distances;
    pcl::KdTreeFLANN<pcl::PointXYZL> tree;
    tree.setInputCloud(label_cloud);
    std::vector<int> overlapping_indices;
    for (size_t p = 0; p < obj.getObjectCloudDS()->size(); ++p) {
        const PointNormal &p_object = obj.getObjectCloudDS()->points[p];
        pcl::PointXYZL p_object_label;
        p_object_label.x = p_object.x; p_object_label.y = p_object.y; p_object_label.z = p_object.z;
        if (!pcl::isFinite(p_object))
            continue;
        if (tree.radiusSearch(p_object_label, 0.02, nn_indices, nn_distances) > 0){
            overlapping_indices.insert(overlapping_indices.end(), nn_indices.begin(), nn_indices.end());
        }
    }
    //sort and remove double indices
    std::sort(overlapping_indices.begin(), overlapping_indices.end());
    overlapping_indices.erase(unique(overlapping_indices.begin(), overlapping_indices.end()), overlapping_indices.end());

    std::vector<int> label_vec;
    for (auto p : overlapping_indices)
        label_vec.push_back(label_cloud->points[p].label);

    //get the most frequent label
    int maxCount = 0, mostElement = *(label_vec.begin());
    int sz = label_vec.size(); // to avoid calculating the size every time
    for(int i=0; i < sz; i++)
    {
        int c = count(label_vec.begin(), label_vec.end(), label_vec.at(i));
        if(c > maxCount)
        {   maxCount = c;
            mostElement = label_vec.at(i);
        }
    }
    return mostElement;
}

void createNewModelFolder(DetectedObject &ro, std::string ppf_model_path) {
    std::string orig_path = ppf_model_path + "/" + std::to_string(ro.getID());
    ro.object_folder_path_ = orig_path;

    boost::filesystem::create_directories(orig_path);
    pcl::io::savePCDFile(orig_path + "/3D_model.pcd", *ro.getObjectCloud()); //PPF will create a new model with the new cloud
}

void removeModelFolder(DetectedObject ro, std::string ppf_model_path, std::string result_path) {
    //there should already exist a folder
    std::string orig_path = ppf_model_path + "/" + std::to_string(ro.getID());
    if (boost::filesystem::exists(orig_path)) {
        boost::filesystem::path dest_folder = result_path + "/model_partially_matched/" + std::to_string(ro.getID());
        boost::filesystem::create_directories(dest_folder);
        //count pcd-files in the folder
        int nr_pcd_files = 0;
        boost::filesystem::directory_iterator end_iter; // Default constructor for an iterator is the end iterator
        for (boost::filesystem::directory_iterator iter(dest_folder); iter != end_iter; ++iter) {
            if (iter->path().extension() == ".pcd")
                ++nr_pcd_files;
        }

        //copy the folder to another directory. The model is already matched and should not be used anymore
        for (const auto& dirEnt : boost::filesystem::recursive_directory_iterator{orig_path})
        {
            if (dirEnt.path().extension() == ".pcd") {
                const auto& path = dirEnt.path();
                boost::filesystem::path dest_filename = path.stem().string() + std::to_string(nr_pcd_files) + path.extension().string();
                boost::filesystem::copy(path, dest_folder / dest_filename);
                std::cout << "Copy " << path << " to " << (dest_folder / path.filename()) << std::endl;
            }
        }
    }
    boost::filesystem::remove_all(orig_path);
}

bool DetectAndMatchObjectsROS::updateDetectedObjects(std::vector<DetectedObject>& ref_result, std::vector<DetectedObject>& curr_result) {
    bool isObjectOrModelNew= false;
    for (DetectedObject ro : ref_result) {
        if (ro.state_ == ObjectState::REMOVED) {
            //means that there was a partial match and have to create a new model folder
            //if (ro.object_folder_path_ == "") {
            if (pot_removed_obj.find(ro.getID()) == pot_removed_obj.end()) {
                createNewModelFolder(ro, ppf_model_path_);
                isObjectOrModelNew= true;
            }
            pot_removed_obj[ro.getID()] = ro;
        } else if (ro.state_ == ObjectState::DISPLACED) {
            //means that there was a partial match and we do not need the model anymore
            if (ro.object_folder_path_ == "") {
                removeModelFolder(ro, ppf_model_path_, debug_path_det_obj_);
            }
            ref_displaced_obj[ro.getID()] = ro;
            pot_removed_obj.erase(ro.getID());
        } else if (ro.state_ == ObjectState::STATIC) {
            //means that there was a partial match and we do not need the model anymore
            if (ro.object_folder_path_ == "") {
                removeModelFolder(ro, ppf_model_path_, debug_path_det_obj_);
            }
            ref_static_obj[ro.getID()] = ro;
            pot_removed_obj.erase(ro.getID());
        }

    }
    for (DetectedObject co : curr_result) {
        if (co.state_ == ObjectState::NEW) {
            if (pot_new_obj.find(co.getID()) == pot_new_obj.end())
                isObjectOrModelNew= true;
            pot_new_obj[co.getID()] = co;
        } else if (co.state_ == ObjectState::DISPLACED) {
            curr_displaced_obj[co.getID()] = co;
            pot_new_obj.erase(co.getID());
        }  else if (co.state_ == ObjectState::STATIC) {
            curr_static_obj[co.getID()] = co;
            pot_new_obj.erase(co.getID());
        }
    }
    return isObjectOrModelNew;
}


void DetectAndMatchObjectsROS::createPPFModelFolders (std::map<int, std::vector<DetectedObject> > &model_objects) {
    //delete corresponding model_objects in ppf_model_folder
//    std::cout << "Deleted model objects from DB with ID ";
//    for (std::map<int, std::vector<DetectedObject> >::iterator it = model_objects.begin(); it != model_objects.end(); it++) {
//        for (DetectedObject o : it->second) {
//            std::string orig_path = ppf_model_path_ + "/" + std::to_string(o.getID());
//            if (boost::filesystem::exists(orig_path)) {
//                boost::filesystem::remove_all(orig_path);
//                std::cout << o.getID() << " ";
//            }
//        }
//    }
//    std::cout << std::endl;

    std::vector<int> good_model_ids;
    for (std::map<int, std::vector<DetectedObject> >::iterator m_it = model_objects.begin(); m_it != model_objects.end(); m_it++) {
        for (DetectedObject &obj : m_it->second) {
            std::string obj_folder = ppf_model_path_ + std::to_string(obj.getID()); //PPF uses the folder name as model_id!

            //check if already stored pcd file has the same size as the cloud from the DB
            pcl::PointCloud<PointNormal>::Ptr stored_pcd_file(new pcl::PointCloud<PointNormal>);
            if (boost::filesystem::exists(obj_folder) && pcl::io::loadPCDFile(obj_folder + "/3D_model.pcd", *stored_pcd_file) == 0) { //successfully read the pcd-file
                if (stored_pcd_file->size() != obj.getObjectCloud()->size()) { //delete ppf-hash-file
                    boost::filesystem::remove_all(obj_folder);
                }
            }

            boost::filesystem::create_directories(obj_folder); //if the folder exist already, it does not get created
            pcl::io::savePCDFile(obj_folder + "/3D_model.pcd", *(obj.getObjectCloud()));
            obj.object_folder_path_ = obj_folder;

            good_model_ids.push_back(obj.getID());
        }
    }

    //delete model folders which do not exist in the vector
    std::cout << "Deleted model object folders with ID ";
    for (boost::filesystem::directory_entry& model_dir : boost::filesystem::directory_iterator(ppf_model_path_)) {
        if (boost::filesystem::is_directory(model_dir) && std::find(good_model_ids.begin(), good_model_ids.end(), std::stoi(model_dir.path().filename().string())) == good_model_ids.end())  {
            boost::filesystem::remove_all(model_dir);
            std::cout << model_dir.path().filename() << " ";
        }
    }
    std::cout << std::endl;
}

void DetectAndMatchObjectsROS::upsampleAndRefineObjects(pcl::PointCloud<PointNormal>::Ptr orig_model_cloud, std::vector<DetectedObject> objects) {
    pcl::octree::OctreePointCloudSearch<PointNormal>::Ptr octree;
    octree.reset(new pcl::octree::OctreePointCloudSearch<PointNormal>(ds_leaf_size_LV));
    octree->setInputCloud(orig_model_cloud);
    octree->addPointsFromInputCloud();

    for (size_t i = 0; i < objects.size(); i++) {
        std::tuple<pcl::PointCloud<PointNormal>::Ptr, std::vector<int> > obj_cloud_ind_tuple= cd_->upsampleObjects(octree,
                                                                                                                   orig_model_cloud, objects[i].getObjectCloud(), debug_path_det_obj_, i);
        pcl::PointCloud<PointNormal>::Ptr upsampled_obj_cloud = get<0>(obj_cloud_ind_tuple);
        cd_->refineNormals(upsampled_obj_cloud);
        objects[i].setObjectCloud(upsampled_obj_cloud);
    }
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

        std::string debug_path_table = debug_path_det_obj_ + "table" + std::to_string(table_it->first);
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
        if (table_numbers.empty() || std::find(table_numbers.begin(), table_numbers.end(), p->object.plane_id) != table_numbers.end()) {
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

    for (std::map<int, std::vector<DetectedObject> >::iterator m_it = model_objects_map.begin(); model_objects_map.end() != m_it; m_it++) {
        if (table_numbers.empty() || std::find(table_numbers.begin(), table_numbers.end(), m_it->first) != table_numbers.end()) {
            const std::vector<DetectedObject> & model_objects =  m_it->second;
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

    pcl::ModelCoefficients::Ptr plane_coeffs(new pcl::ModelCoefficients);
    table_extractor::Plane plane_msg = obj_msg.plane_coeffs;
    std::vector<float> coeff_vec {plane_msg.x, plane_msg.y, plane_msg.z, plane_msg.d};
    plane_coeffs->values = coeff_vec;

    DetectedObject obj(obj_msg.id, obj_cloud, plane_cloud, plane_coeffs, ObjectState::UNKNOWN, obj_msg.object_path);

    int table_id = obj_msg.plane_id;

    return std::make_tuple(table_id, obj);
}

obj_det_ppf_matching_msgs::Object DetectAndMatchObjectsROS::fromDetObjToObjMsg(const DetectedObject obj, int table_id) {
    obj_det_ppf_matching_msgs::Object obj_msg;

    obj_msg.id = obj.getID();
    obj_msg.plane_id = table_id;

    sensor_msgs::PointCloud2 obj_cloud_msg;
    pcl::toROSMsg(*obj.getObjectCloud(), obj_cloud_msg);
    obj_msg.obj_cloud = obj_cloud_msg;

    sensor_msgs::PointCloud2 plane_cloud_msg;
    pcl::toROSMsg(*(obj.plane_cloud_), plane_cloud_msg);
    obj_msg.plane_cloud = plane_cloud_msg;

    table_extractor::Plane plane_msg;
    plane_msg.x = obj.plane_coeffs_->values[0];
    plane_msg.y = obj.plane_coeffs_->values[1];
    plane_msg.z = obj.plane_coeffs_->values[2];
    plane_msg.d = obj.plane_coeffs_->values[3];
    obj_msg.plane_coeffs = plane_msg;

    obj_msg.object_path = obj.object_folder_path_.c_str();

    return obj_msg;
}

obj_det_ppf_matching_msgs::CandidateObject DetectAndMatchObjectsROS::fromDetObjToCandidateObjMsg(const DetectedObject obj, int table_id) {
    obj_det_ppf_matching_msgs::Object obj_msg = fromDetObjToObjMsg(obj, table_id);

    obj_det_ppf_matching_msgs::CandidateObject candidate_obj_msg;
    candidate_obj_msg.object = obj_msg;
    candidate_obj_msg.state = obj_det_ppf_matching_msgs::ObjectStateClass::NEW;

    obj_det_ppf_matching_msgs::ObjectMatch match_msg;
    match_msg.model_id = obj.match_.model_id;
    match_msg.object_id = obj.match_.object_id;

    //convert Eigen4f to geometry_msg/Transform(vector+quaternion)
    geometry_msgs::Transform tf_msg;
    Eigen::Matrix3f rotation = obj.match_.transform.block<3, 3>(0, 0);
    Eigen::Quaternionf quat(rotation);
    tf_msg.rotation.x = quat.x(); tf_msg.rotation.y = quat.y(); tf_msg.rotation.z = quat.z(); tf_msg.rotation.w = quat.w();

    Vector4f_NotAligned trans = obj.match_.transform.block<4, 1>(0, 3);
    tf_msg.translation.x = trans[0]; tf_msg.translation.y = trans[1]; tf_msg.translation.z = trans[2];

    match_msg.transform = tf_msg;

    candidate_obj_msg.match = match_msg;

    return candidate_obj_msg;
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
    //optional, otherwise it will be stored in the working direcotry
    nh_->getParam("ppf_model_folder", ppf_model_path_);
    nh_->getParam("debug_path_det_obj", debug_path_det_obj_);

    //setup the .ros directory if it does not exist and use it if not paths are given for ppf_model_folder and debug_path_det_obj
    std::string ros_log_env;
    ros::get_environment_variable(ros_log_env, "ROS_HOME");
    if (ros_log_env == "") {
        const char *homedir;
        if ((homedir = getenv("HOME")) == NULL) {
            homedir = getpwuid(getuid())->pw_dir;
        }
        ros_log_env = std::string(homedir) + "/.ros/obj_det_ppf_matching/";
        boost::filesystem::create_directory(ros_log_env);
    }

    if (debug_path_det_obj_ != "")
        debug_path_det_obj_ += "/";
    else
        debug_path_det_obj_ = ros_log_env;

    if (ppf_model_path_  != "")
        ppf_model_path_ += "/";
    else
        ppf_model_path_ = ros_log_env;

    extract_perm_objs_  = nh_->advertiseService ("extract_perm_objects", &DetectAndMatchObjectsROS::extractPermanentObjects, this);
    det_and_compare_objects_  = nh_->advertiseService ("det_and_compare_objects", &DetectAndMatchObjectsROS::detectAndCompareObjects, this);

    //Create ChangeDetection object
    cd_.reset(new ChangeDetection (ppf_config_path_));
    ppf_model_path_ += "model_objects/";
    if (!boost::filesystem::exists((ppf_model_path_)))
        boost::filesystem::create_directories(ppf_model_path_);
    cd_->setPPFModelPath(ppf_model_path_);

    debug_path_det_obj_ += "debug_output/";
    if (boost::filesystem::exists((debug_path_det_obj_)))
        boost::filesystem::remove_all(debug_path_det_obj_);
    boost::filesystem::create_directories(debug_path_det_obj_);
    cd_->setOutputPath(debug_path_det_obj_);
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

