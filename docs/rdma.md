# RDMA

RDMA provides low latency through stack bypass and copy avoidance, reduces CPU
utilization, reduces memory bandwidth bottlenecks and provides high bandwidth
utilization. RDMA uses the OS to establish a channel and then allows
applications to directly exchange messages without further OS intervention.

In order to perform RDMA operations, establishment of a connection to the remote
host, as well as appropriate permissions need to be set up first. The mechanism
for accomplishing this is the Queue Pair (QP). A QP is roughly equivalent to a
socket. The QP needs to be initialized on both sides of the connection.
Communication Manager (CM) can be used to exchange information about the QP
prior to actual QP setup. Once a QP is established, the verbs API can be used
to perform RDMA reads, RDMA writes, and atomic operations.

In an RDMA Read, a section of memory is read from the remote host. The caller
specifies the remote virtual address as well as a local memory address to be
copied to. Prior to performing RDMA operations, the remote host must provide
appropriate permissions to access its memory. Once these permissions are set,
RDMA read operations are conducted with no notification whatsoever to the remote
host. For both RDMA read and write, the remote side isn't aware that this
operation being done (other than the preparation of the permissions and
resources).

### What is an anonymous page mapping?

Anonymous memory is a memory mapping with no file or device backing it. This is
how programs allocate memory from the operating system for use by things like
the stack and heap. Initially, an anonymous mapping only allocates virtual
memory. The new mapping starts with a redundant copy on write mapping of the
zero page (i.e. a single page of physical memory filled with zeros, maintained
by the OS). Attempts to write to the page trigger the normal copy-on-write
mechanism in the page fault handler, allocating fresh memory only when needed
to allow the write to proceed. Thus "dirtying" anonymous pages allocates
physical memory, the actual allocation call only allocates virtual memory.
Dirty anonymous pages can be written to swap space.
[source](https://landley.net/writing/memory-faq.txt)
