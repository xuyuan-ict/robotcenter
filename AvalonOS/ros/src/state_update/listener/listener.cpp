/*
 * Copyright (C) 2008, Morgan Quigley and Willow Garage, Inc.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the names of Stanford University or Willow Garage, Inc. nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <iostream>
#include "ros/ros.h"
#include "nav_msgs/Odometry.h"

/**
 * This tutorial demonstrates simple receipt of messages over the ROS system.
 */
void odomParse(const nav_msgs::Odometry::ConstPtr& msg)
{
  // ROS_INFO("I heard: [%s]", msg->pose.c_str());
  // std::cout << msg->pose.pose << std::endl;;
}


int main(int argc, char **argv)
{
  std::cout << argc << std::endl;
  for(size_t i=1; i<argc; ++i)
  	std::cout << argv[i] << std::endl;
  ros::init(argc, argv, "listener");

  ros::NodeHandle n;


  ros::Subscriber sub_robot1 = n.subscribe("turtlebot1/odom", 1000, odomParse);
  ros::Subscriber sub_robot2 = n.subscribe("turtlebot2/odom", 1000, odomParse);
  ros::Subscriber sub_robot3 = n.subscribe("uav1/ground_truth/state", 1000, odomParse);

  ros::spin();


  return 0;
}
