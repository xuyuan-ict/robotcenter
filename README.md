# robotcenter


# Building

```
  $ cp -r avalon_simulator/avalon* <your catkin workspace>/src/
  $ cd AvalonOS
  $ mkdir build
  $ cd build
  $ ../configure
  $ make
  $ make check
```

# Test

## start up gazebo simulator:
```
  $ roslaunch avalon_gazebo avalon_multirobot_demo.launch
```
## start up rviz
```
  $ roslaunch avalon_rviz view_multi_navigation.launch
```
## create Avalon work directory
```
  $ mkdir /var/lib/avalon
```
## start Avalon master (Ensure work directory has proper permissions).
```
  $ cd AvalonOS/build
  $ ./master
```
## start Avalon slave (Ensure work directory has proper permissions).
```
  $ ./slave1
  $ ./slave2
  $ ./slave3
```

## Run test C++ framework
```
  $ ./test-framework
```
select `Publish Point` in rviz and select a point with the left mouse button
