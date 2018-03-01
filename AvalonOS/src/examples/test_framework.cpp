/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <libgen.h>

#include <iostream>
#include <string>

#include <boost/lexical_cast.hpp>
#include <avalon/scheduler.hpp>

#include "common/robot.hpp"

#include <stout/os.hpp>
#include <stout/stringify.hpp>

using namespace avalon;
using namespace avalon::internal::robot;

using boost::lexical_cast;

using std::cout;
using std::cerr;
using std::endl;
using std::flush;
using std::string;
using std::vector;

const int32_t CPUS_PER_TASK = 1;
const int32_t MEM_PER_TASK = 32;

//------------@Ailias Begin----------//
const int32_t TWO_HAND_PER_TASK = 2;
const int32_t TWO_FOOT_PER_TASK = 2;
const int32_t ONE_CAMERA_PER_TASK = 1;
const int32_t FOUR_WHEEL_PER_TASK = 4;
const int32_t ONE_THREEDCAMERA_PER_TASK = 1;
//-----------@Ailias End------------//

class TestScheduler : public Scheduler
{
public:
  TestScheduler(const ExecutorInfo& _executor)
    : executor(_executor),
      tasksLaunched(0),
      tasksFinished(0),
      totalTasks(1) {}

  virtual ~TestScheduler() {}

  virtual void registered(SchedulerDriver*,
                          const FrameworkID&,
                          const MasterInfo&)
  {
    cout << "Registered!" << endl;
  }

  virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo) {}

  virtual void disconnected(SchedulerDriver* driver) {}

  virtual void resourceOffers(SchedulerDriver* driver,
                              const vector<Offer>& offers)
  {
    cout << "." << flush;
    for (size_t i = 0; i < offers.size(); i++) {
      const Offer& offer = offers[i];
      const Pose& pose = offer.pose();

      cout << "slave_id: " << offer.slave_id().value() << endl;

      vector<TaskInfo> tasks;
      while(tasksLaunched < totalTasks) {
        int taskId = tasksLaunched++;

        cout << "Starting task " << taskId << " on "
             << offer.hostname() << endl;

        TaskInfo task;
        task.set_name("Task " + lexical_cast<string>(taskId));
        task.mutable_task_id()->set_value(lexical_cast<string>(taskId));
        task.mutable_slave_id()->MergeFrom(offer.slave_id());
        task.mutable_executor()->MergeFrom(executor);
        task.mutable_executor()->set_source("task_" + stringify(taskId));
        task.set_robot_name(offer.robot_name());

        Resource* resource;

        resource = task.add_resources();
        resource->set_name("cpus");
        resource->set_type(Value::SCALAR);
        resource->set_rtype(CRES);
        resource->mutable_scalar()->set_value(CPUS_PER_TASK);

        resource = task.add_resources();
        resource->set_name("mem");
        resource->set_type(Value::SCALAR);
        resource->set_rtype(CRES);
        resource->mutable_scalar()->set_value(MEM_PER_TASK);

        resource = task.add_resources();
        resource->set_name("kinect");
        resource->set_type(Value::SET);
        resource->set_rtype(DRES);
        resource->mutable_set()->add_item("ImageGen");

        resource = task.add_resources();
        resource->set_name("wheel");
        resource->set_type(Value::SET);
        resource->set_rtype(ARES);
        resource->mutable_set()->add_item("Move");

        tasks.push_back(task);
      }

      driver->launchTasks(offer.id(), tasks);
    }
  }

  // virtual void offerRescinded(SchedulerDriver* driver,
  //                             const OfferID& offerId) {}

  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
  {
    int taskId = lexical_cast<int>(status.task_id().value());
    cout << "Task " << taskId << " is in state " << status.state() << endl;

    if (status.state() == TASK_FINISHED)
      tasksFinished++;

    if (tasksFinished == totalTasks)
      driver->stop();
  }

  // virtual void frameworkMessage(SchedulerDriver* driver,
  //                               const ExecutorID& executorId,
  //                               const SlaveID& slaveId,
  //                               const string& data) {}

  // virtual void slaveLost(SchedulerDriver* driver, const SlaveID& sid) {}

  // virtual void executorLost(SchedulerDriver* driver,
  //                           const ExecutorID& executorID,
  //                           const SlaveID& slaveID,
  //                           int status) {}

  virtual void error(SchedulerDriver* driver, const string& message) {}

private:
  const ExecutorInfo executor;
  int tasksLaunched;
  int tasksFinished;
  int totalTasks;
};


int main(int argc, char** argv)
{
  if (argc != 3) {
    cerr << "Usage: " << argv[0] << " <master>" << " <--range=>" <<endl;
    return -1;
  }

  // Find this executable's directory to locate executor.
  char * build_path = ".";
  string uri = string(os::realpath(dirname(build_path)).get()) 
                + "/src/test-executor";
  if (getenv("AVALON_BUILD_DIR")) {
    uri = string(getenv("AVALON_BUILD_DIR")) + "/src/test-executor";
  }
  cout << "test-executor uri: " << uri << endl;

  Position position = getFWPoint();

  printRobotPosition(position);

  ExecutorInfo executor;
  executor.mutable_executor_id()->set_value("default");
  executor.mutable_command()->set_value(uri);
  executor.set_name("Test Executor (C++)");

  TestScheduler scheduler(executor);

  FrameworkInfo framework;
  framework.set_user(""); // Have Avalon fill in the current user.
  framework.set_name("Test Framework (C++)");
  framework.mutable_position()->MergeFrom(position);
  framework.set_range(atoi(argv[2]));

  AvalonSchedulerDriver driver(&scheduler, framework, argv[1]);

  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
