# MapReduce-Engine-and-Distributed-File-System

Our MapReduce facility has the following features:

1) We use human-readable and human-editable configuration files so that the users are
able to congure their instances, including identifying the IP address, port numbers, etc.

2) We use efficient schedule algorithm to dispatch map and reduce tasks, and hence we
maximized the performance gain through parallelism within each phase.

3) Health Check and Fault Tolerance. In our system, master node and slave node keep
communication through heartbeat mechanism. Once one slave is down, we will track back
its current task list and reschedule these tasks.

4) For Users and Developers: we provide a general-purpose I/O facility to support the
necessary operations. And the developer only need to implements our mapper and reducer
interface. Also we provide management tools for users. They only need to type several
commands such as start, monitor, and stop to manage the start-up and shut-down of our
engine.

We tested our MapReduce facility on two test cases. Through step by step running and
checking our engine, it achieves the original design perfectly.

The Repository is only used for study. All rights reserved.
