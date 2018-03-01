//
// Created by Ailias on 3/13/17.
//

#ifndef __ROBOT_HPP__
#define __ROBOT_HPP__

#include <unistd.h>
#include <sys/wait.h>
#include <fstream>
#include <string>
#include <vector>
#include <cstring>
#include <cmath>
#include <map>
#include <avalon/avalon.hpp>
#include <stout/os.hpp>

#include "values.hpp"

namespace avalon{
namespace internal{
namespace robot{
using namespace std;
using namespace os;


inline void getLocation(char ** location, string robotName, 
                        string odomName) {
  string fPath = string(getenv("AVALON_BUILD_DIR")) + "/ros-rtloc";
  string OdomTopic = robotName + "/" + odomName;
  char *name = new char[OdomTopic.length()+1];
  char *fName = new char[fPath.length()+1];
  strcpy(name, OdomTopic.c_str());
  strcpy(fName,fPath.c_str());

  pid_t pid;
  if ((pid = vfork()) == 0) {
    char *argv[] = {"/bin/sh", fName, name,  NULL};
    if ((execv("/bin/sh", argv) == -1)) {
      perror("cannot get location");
    }
    _exit(0);
  }
	   
  int status;
  if(wait(&status)==pid){
    int num;
    std::ifstream fin("robot.loc", std::ios::in);
    for (num = 0; num < 13; ++num) {
      fin.getline(location[num], 32);
    }
    fin.close();
  }
}


inline Position getRobotPositionFromLocation(char** location) {
  Position position;
  position.set_x(atof(location[0]));
  position.set_y(atof(location[1]));
  position.set_z(atof(location[2]));

  return position;
}


inline Orientation getRobotOrientationFromLocation(char** location) {
  Orientation orientation;
  orientation.set_x(atof(location[3]));
  orientation.set_y(atof(location[4]));
  orientation.set_z(atof(location[5]));
  orientation.set_w(atof(location[6]));

  return orientation;
}


inline Pose getRobotPoseFromLocation(char** location) {
  Position position = getRobotPositionFromLocation(location);
  Orientation orientation = getRobotOrientationFromLocation(location);
  
  Pose pose;
  pose.mutable_position()->MergeFrom(position);
  pose.mutable_orientation()->MergeFrom(orientation);
  return pose;
}


inline Twist getRobotTwistFromLocation(char** location) {
  Linear linear;
  linear.set_x(atof(location[7]));
  linear.set_y(atof(location[8]));
  linear.set_z(atof(location[9]));

  Angular angular;
  angular.set_x(atof(location[10]));
  angular.set_y(atof(location[11]));
  angular.set_z(atof(location[12]));

  Twist twist;
  twist.mutable_linear()->MergeFrom(linear);
  twist.mutable_angular()->MergeFrom(angular);
  return twist;
}


/**
*construct a RobotStatus structure from location array info
*used for executor: [launchTask/sendStatusUpdate]
*/
inline RobotStatus getRobotStatusFromLocation(char** location) {
  Pose pose = getRobotPoseFromLocation(location);
  Twist twist = getRobotTwistFromLocation(location);

  RobotStatus robotStatus;
  robotStatus.mutable_pose()->MergeFrom(pose);
  robotStatus.mutable_twist()->MergeFrom(twist);
  return robotStatus;
}


inline Pose robotParse(const string& s) {
  // Tokenize and parse the value of "pose".
  // char **data;

  // vector<string> tokens = strings::tokenize(s, ";\n");

  // for (size_t i = 0; i < 7; ++i) {
  //   const vector<string>& pairs = strings::tokenize(tokens[i], ",");
  //   // if (pairs.size() != 7) {
  //   //   LOG(FATAL) << "Bad value for pose, missing ',' among each value";
  //   // }
  //   data[i] = pairs[i];
  // }

  // return getRobotPoseFromLocation(data);
}


inline Position robotTfParse(const string& s) {
  // Tokenize and parse the value of "pose".
  char **data = (char **)malloc(sizeof(char *)*3);

  const vector<string> pos = strings::tokenize(s, ",");
  cout << "tokenize: " << s << endl;

  for (size_t i = 0; i < pos.size(); ++i) {
    data[i] = new char[pos[i].length()+2];
    strcpy(data[i], pos[i].c_str());
  }

  return getRobotPositionFromLocation(data);
}


inline void tfRobotPose(Pose& pose, Position& tf) {
  Position pos = pose.position();
  pos.set_x(pos.x() + tf.x());
  pos.set_y(pos.y() + tf.y());
  pos.set_z(pos.z() + tf.z());
  pose.mutable_position()->MergeFrom(pos);
}


void printRobotPosition(Position position) {
  LOG(INFO) << "Position:";
  LOG(INFO) << "    x: " << position.x();
  LOG(INFO) << "    y: " << position.y();
  LOG(INFO) << "    z: " << position.z();
}


void printRobotOrientation(Orientation orientation) {
  LOG(INFO) << "Orientation:";
  LOG(INFO) << "    x: " << orientation.x();
  LOG(INFO) << "    y: " << orientation.y();
  LOG(INFO) << "    z: " << orientation.z();
  LOG(INFO) << "    w: " << orientation.w();
}


void printRobotPose(Pose pose) {
  printRobotPosition(pose.position());
  printRobotOrientation(pose.orientation());
}


inline Position getFWPoint() {
  char fName[100];//change the const char* to char*
  string fPath = "./ros-fwloc-tmp";
  strcpy(fName,fPath.c_str());
        
  pid_t pid;
  if ((pid = vfork()) == 0) {
    char *argv[] = {"/bin/sh", fName, NULL,  NULL};
    if ((execv("/bin/sh", argv) == -1)) {
      perror("cannot get location");
    }
    _exit(0);
  }

  sleep(1);
  std::ifstream fin("fw-tmp.loc", std::ios::in);
  while(1) {
    fin.seekg (0, fin.end);
    int length = fin.tellg();
    fin.seekg (0, fin.beg);

    if(length > 0) {
      kill(pid ,SIGINT);
      break;
    }
  }

  fin.close();

  fPath = "./ros-fwloc";
  strcpy(fName,fPath.c_str());
        
  if ((pid = vfork()) == 0) {
    char *argv[] = {"/bin/sh", fName, NULL,  NULL};
    if ((execv("/bin/sh", argv) == -1)) {
      perror("cannot get location");
    }
    _exit(0);
  }

  int status;
  char ** location = (char**)malloc(sizeof(char*)*13);
    for(int i=0; i<13; i++)
        location[i] = (char*)malloc(sizeof(char)*32);

  if(wait(&status) == pid) {
    std::ifstream fin("fw.loc", std::ios::in);
    for (size_t num = 0; num < 3; ++num) {
      fin.getline(location[num], 32);
    }
    fin.close();
  }

  Position position;
  position.set_x(atof(location[0]));
  position.set_y(atof(location[1]));
  position.set_z(atof(location[2]));

  for(int i=0; i<13; i++)
    free(location[i]);
  free(location);

  return position;

}

inline std::string lxcGetContainerIP(const std::string& container_name) {
  std::string ip;
  string fPath = string(getenv("AVALON_BUILD_DIR")) + "/container-ip";
  char *name = new char[container_name.length()+1];
  char *fName = new char[fPath.length()+1];
  strcpy(name, container_name.c_str());
  strcpy(fName,fPath.c_str());

  pid_t pid;
  if ((pid = vfork()) == 0) {
    char *argv[] = {"/bin/sh", fName, name,  NULL};
    if ((execv("/bin/sh", argv) == -1)) {
      perror("cannot get container ip");
    }
    _exit(0);
  }
     
  int status;
  if(wait(&status)==pid){
    int num;
    std::ifstream fin("container.ip", std::ios::in);
    getline(fin, ip);
    fin.close();
    os::rm("container.ip");
  }

  return ip;
}


/**
/*
* construt model_state rosservice cmd string
/
  inline char* getRobotModelStateCMD(const RobotStatus& robotStatus){
      Position position = robotStatus.pose().position();
      Orientation orientation = robotStatus.pose().orientation();
      Linear linear = robotStatus.twist().linear();
      Angular angular = robotStatus.twist().angular();
      char model_state_str[200] ;
      sprintf(model_state_str,"rosservice call /gazebo/set_model_state '{model_state: { model_name: $1, pose: { position: { x: %f, y: %f ,z: %f }, orientation: {x: %f, y: %f, z: %f, w: %f } }, twist: { linear: {x: %f , y: %f ,z: %f } , angular: { x: %f , y: %f , z: %f } } } }'",
            position.x(), position.y(), position.z(), 
            orientation.x(), orientation.y(), orientation.z(), orientation.w(),
            linear.x(), linear.y(), linear.z(),
            angular.x(), angular.y(), angular.z());
      return model_state_str;
  }

/**
* execute robot model_state cmd in gazebo simulator 
/
  inline bool setRobotModelState(char * robot_name, char* cmd){
    ofstream cmdFile(robot_name);
    if(cmdFile){
      cmdFile << cmd;
      cmdFile.close();  
    }
    pid_t pid;
    if ((pid = vfork()) == 0) {
       char *argv[] = {"/bin/sh", robot_name, NULL};
       if ((execv("/bin/sh", argv) == -1)) {
           perror("cannot set model_state");
       }
       _exit(0);
    }
    int pstatus;
    if (wait(&pstatus) == pid){
         return true;
    }
    return false;
  }
*/


/**
*check whether the two elements are different
*/
inline bool checkRobotStatusDiff(RobotStatus prev, RobotStatus post){
  Pose prevPose = prev.pose();
  Pose postPose = post.pose();
  double totDiff = abs(prevPose.position().x() - postPose.position().x()) + 
                   abs(prevPose.position().y() - postPose.position().y()) + 
                   abs(prevPose.position().z() - postPose.position().z());
  std::cout<<"robotStatus diff="<<totDiff<<std::endl;
  return totDiff >= 0.01;
}


/**
 * write the robot name to the file
 * @param robot_name
 */
inline void writeRobotName(std::string robot_name) {
  char fName[100];
  string fPath = string(getenv("AVALON_BUILD_DIR")) + "/robot.name";
  strcpy(fName, fPath.c_str());
  std::ofstream outFile(fName, std::ios::out);
  if (outFile) {
    outFile << robot_name;
    outFile.close();
  }
  else
    std::cerr << "[Error]:open robot.name file error.\n";
}


/**
 * read the robot name from the file
 * @return robot_name
 */
inline string readRobotName() {
  char fName[100];
  string fPath = string(getenv("AVALON_BUILD_DIR")) + "/robot.name";
  strcpy(fName,fPath.c_str());
  string robot_name;
  ifstream inFile(fName, std::ios::in);
  if (inFile) {
    inFile >> robot_name;
    inFile.close();
  } else
    std::cerr << "[Error]:open robot.name file error.\n";
    return robot_name;
}


/**
 * read the robot capability file to the slave and send to the master
 */
inline std::map<std::string, std::string> getRobotResources(){
  char fName[100];
  string fPath = string(getenv("AVALON_BUILD_DIR")) + "/robot.res";
  strcpy(fName,fPath.c_str());
  std::ifstream inFile(fName,std::ios::in);
        
  std::map<std::string,std::string> robot_res;
  if(inFile){
    while(!inFile.eof()){
      std::string res_name = "";
      std::string res_value="";
      inFile>>res_name>>res_value;
      if(res_name !="" && res_value !="") {
        robot_res.insert(std::pair<std::string, std::string>(res_name, res_value));
      }else{
        std::cerr<<"robot resource should be pair each line\n";
      }
    }

    inFile.close();

  }else{
    std::cerr << "[Error]:open robot.res file error.\n";
    exit(-1);
  }
  return robot_res;
}


}//end robot
}//end internal
}//end avalon

#endif //__ROBOT_HPP__
