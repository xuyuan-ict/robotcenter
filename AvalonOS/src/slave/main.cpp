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

#include <stout/os.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>

#include "common/build.hpp"
#include "common/robot.hpp"

#include "configurator/configuration.hpp"
#include "configurator/configurator.hpp"

#include "detector/detector.hpp"

#include "flags/flags.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "slave/isolation_module_factory.hpp"
#include "slave/slave.hpp"

using namespace avalon::internal;
using namespace avalon::internal::slave;
using namespace avalon::internal::robot;

using std::cerr;
using std::endl;
using std::string;


void usage(const char* argv0, const Configurator& configurator)
{
  cerr << "Usage: " << os::basename(argv0).get() << " [...]" << endl
       << endl
       << "Supported options:" << endl
       << configurator.getUsage();
}


int main(int argc, char** argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  flags::Flags<logging::Flags, slave::Flags> flags;

  // The following flags are executable specific (e.g., since we only
  // have one instance of libprocess per execution, we only want to
  // advertise the port and ip option once, here).
  Option<uint16_t> port;
  flags.add(&port, "port", "Port to listen on");

  Option<string> ip;
  flags.add(&ip, "ip", "IP address to listen on");

  Option<string> isolation;
  flags.add(&isolation, "isolation", "Isolation module");

  Option<string> master;
  flags.add(&master,
            "master",
            "May be one of:\n"
            "  zk://host1:port1,host2:port2,.../path\n"
            "  zk://username:password@host1:port1,host2:port2,.../path\n"
            "  file://path/to/file (where file contains one of the above)");

  Option <string> robot_name;
  flags.add(&robot_name, "robot_name", "The connected robot name");

  Option <string> type;
  flags.add(&type, "type", "The connected robot type. eg: AGV, UAV");

  bool help;
  flags.add(&help,
            "help",
            "Prints this help message",
            false);

  Configurator configurator(flags);
  Configuration configuration;
  try {
    configuration = configurator.load(argc, argv);
  } catch (ConfigurationException& e) {
    cerr << "Configuration error: " << e.what() << endl;
    usage(argv[0], configurator);
    exit(1);
  }

  flags.load(configuration.getMap());

  if (help) {
    usage(argv[0], configurator);
    exit(1);
  }

  if (master.isNone()) {
    cerr << "Missing required option --master" << endl;
    exit(1);
  }

  // Initialize libprocess.
  if (port.isSome()) {
    os::setenv("LIBPROCESS_PORT", stringify(port.get()));
  }

  if (ip.isSome()) {
    os::setenv("LIBPROCESS_IP", ip.get());
  }

  if (isolation.isSome()) {
    os::setenv("ISOLATION_TYPE", isolation.get());
  }

  if (robot_name.isNone()) {
    cerr << "Missing required option --robot_name" << endl;
    exit(1);
  } else {
    os::setenv("AVALON_ROBOT_NAME", robot_name.get());
  }

  if (type.isSome()) {
    os::setenv("AVALON_ROBOT_TYPE", type.get());
  }

  char * build_path = ".";
  setenv("AVALON_BUILD_DIR",os::realpath(dirname(build_path)).get(), false);
  
  //write the input robot name to name.rob file
  // writeRobotName(robot_name.get());

  process::initialize();

  logging::initialize(argv[0], flags);

  LOG(INFO) << "Creating \"" << isolation.get() << "\" isolation module";

  IsolationModule* isolationModule = IsolationModule::create(isolation.get());
  if (isolationModule == NULL) {
    cerr << "Unrecognized isolation type: " << isolation.get() << endl;
    exit(1);
  }

  LOG(INFO) << "Build: " << build::DATE << " by " << build::USER;
  LOG(INFO) << "Starting Avalon slave";

  Files files;
  Slave* slave = new Slave(flags, false, isolationModule, &files);
  process::spawn(slave);

  Try<MasterDetector*> detector =
    MasterDetector::create(master.get(), slave->self(), false, flags.quiet);

  CHECK_SOME(detector) << "Failed to create a master detector";

  process::wait(slave->self());
  delete slave;

  MasterDetector::destroy(detector.get());
  IsolationModule::destroy(isolationModule);

  return 0;
}