### What is a block device?
Block devices are characterized by random access to data organized in fixed
size blocks. Examples of such devices are hard drives, RAM disks, etc.. The
Linux kernel handles differently those two types of devices. Character devices
have a single current position, while block devices must be able to move to any
position in the device to provide random access to data. To simplify work with
block devices, the Linux kernel provides an entire subsystem called the block
I/O (or block layer) subsystem.

From the kernel persepctive, the smallest logical unit of addressing is the
block. The smallest unit on a block device is a sector. Although the physical
device can be addressed at sector level, the kernel performs all disk operations
using blocks. Since the smallest unit of physical addressing is the sector, the
size of the block must be a multiple of the size of the sector. Additionally,
the block size must not exceed the size of a page.

### How is block I/O represented in the kernel?
The basic container for block I/O within the kernel is the `bio` structure,
which is defined in `<linux/blk_types.h>`. This structure represents block I/O
operations that are in-flight (active) as a list of segments. A segment is a
chunk of a buffer that is contiguous in memory. By allowing the buffers to be
described in chunks, the `bio` structure provides the capability for the kernel
to perform block I/O operations of even a single buffer from multiple locations
in memory. Vector I/O such as this is called scatter-gather I/O.

The primary purpose of a `bio` structure is to represent an in-flight block I/O
operation. To this end, the majority of the fields in the structure are
housekeeping related.

The `bi_io_vec` field points to an array of `bio_vec` structures. These
structures are used as lists of individual segments in this specific block I/O
operation. As the block I/O operation is carried out, the `bi_idx` field is
used to point to the current index into the array.

```
struct bio {
	struct bio *bi_next;  // list of requests
	struct block_device *bi_bdev; // associated block dev, defined in <linux/fs.h>
	unsigned int bi_flags;	// status and command flags
	unsigned int bi_rw;  // bottom bits READ/WRITE, top bits priority
	unsigned short bi_phys_segments;  // number of segments
	...
	struct bvec_iter bi_iter;
	...
	unsigned short 		 bi_vcnt;  	 // how many bio_vec's
	struct bio_vec    *bi_io_vec;  // the actual vec list
	...
};

struct bvec_iter {
	sector_t bi_sector;  // device address in 512 byte
	unsigned int bi_size;  // residual I/O count
	unsigned int bi_idx;	// current index into bvl_vec
	unsigned int bi_bvec_done;	// number of bytes completed in current bvec
};

struct bio_vec {
	struct page *bv_page;		// pointer to physical page on which buffer resides
	unsigned int bv_offset; // offset within the page where buffer resides
	unsigned int bv_len;		// length in bytes of this buffer
};
```

[Ref = https://www.doc-developpement-durable.org/file/Projets-informatiques/cours-&-manuels-informatiques/Linux/Linux%20Kernel%20Development,%203rd%20Edition.pdf - Page 295]

In summary, each block I/O requests is represented by a `bio` structure. Each
request is composed of one or more blocks, which are stored in an array of
`bio_vec` structures. The `bio` structure makes it easy to perform
scatter-gather (vectored) block I/O operations, with the data involved in the
operation originating from multiple physical pages.


## Request Queues
Block devices store their pending block I/O requests in what is called
_request queue_. The _request queue_ is represented in the kernel as the
`request_queue` struct and is defined in `<linux/blkdev.h>`. The request queue
contains a doubly linked list of requests and associated control information.
Requests are added to the queue by higher-level code in the kernel, such as
filesystems and (???) probably swap manager (e.g. VMM adding BIOs to the
request queue associated with the swap device).

The block device driver is responsible to pop a request from a non-empty queue
and submit it to its associated block device. Each item in the queue's request
list is a single request of type `struct request`. Each requests can contain
several block IOs. However,

```

typedef void (request_fn_proc) (struct request_queue *q);
typedef blk_qc_t (make_request_fn) (struct request_queue *q, struct bio *bio);
typedef int (prep_rq_fn) (struct request_queue *, struct request *);

struct request_queue {

	request_fn_proc  *request_fn;
	make_request_fn *make_request_fn;
	prep_req_fn *prep_rq_fn;


	struct blk_mq_ops *mq_ops;

	/* software queues */
	struct blk_mq_ctx __percpu *queue_ctx;
	unsigned int nr_queues;

	/* hardware dispatch queues */
	struct blk_mq_hw_ctx **queue_hw_ctx;
	unsigned int nr_hw_queues;

	struct queue_limits limits;  // probably useful

	struct blk_queue_tag *queue_tags;

	struct blk_mq_tag_set *tag_set;

};

struct request {
	...
	struct request_queue *q;
	struct blk_mq_ctx *mq_ctx;
	...
	struct bio *bio;
	struct gendisk *rq_disk;
	...
	int tag;

	rq_end_io_fn *end_io;  // completion callback
};
```


### Block layer or I/O Linux subsystem
[TODO: get a better understanding of I/O subsystem in Linux]
Applications submit I/Os via a kernel system call, that converts them into a
data structure called a block I/O. Each block I/O contains information such as
I/O address, I/O size, I/O mode (r/w) or I/O type (synchronous/asynchronous).
It is then transferred to either libaio for asynchronous I/Os or directly to
the block layer for synchronous IO that submit it to the block layer. Once an
I/O is submitted, the corresponding block IO is buffered in the staging area,
which is implemented as a queue, denoted the _request queue_.

Once a request is in the staging area, the block layer may perform I/O
scheduling and adjust accounting information before scheduling IO submissions
to the appropriate storage device driver. However, the multi-queue block layer
allows block I/O requests to be maintained in a collection of one or more
request queues. These staging queues can be configured such that there is one
such queue per socket, or per core, on the system.

After I/O has entered the staging queues, there is a new intermediate layer
known as the hardware dispatch queues. Using these queues block I/Os scheduled
for dispatch are not sent directly to the device driver, they are instead sent
to the hardware dispatch queue. The number of hardware dispatch queues will
typically match the number of hardware contexts supported by the device driver.
Device drivers may choose to support anywhere from one to 2048 queues. Because
I/O ordering is not supported within the block layer any software queue may
feed any hardware queue without needing to maintain a global ordering.
**The size of the hardware dispatch queue is bounded and correspond to the
maximum queue depth that is supported by the device driver and hardware.**

In  our  design,  we  build  upon  this  tagging  notion  by  allowing the
block layer to generate a unique tag associated with an IO that is inserted
into the hardware dispatch queue (between size 0 and the max dispatch queue
size). This tag is then re-used by the device driver (rather than generating
a new one, as with NCQ). Upon completion this same tag can then be used by both
the device driver and the block layer to identify completions without the need
for redundant tagging. While  the  MQ  implementation  could  maintain  a
traditional in-flight list for legacy drivers, high IOPS drivers will likely
need to make use of tagged IO to scale well.


The VMM issues BIOs (block I/Os) to the block layer of the kernel. BIOs is the
main unit of I/O for the block layer and lower layers (i.e. drivers and
stacking drivers). Those BIOs are defined in file `include/linux/blk_types.h`
and a simplistic definition of this struct is shown below:
```
struct block_device {
	dev_t bd_dev;
	...
	struct gendisk *bd_disk;
	struct request_queue *bd_queue;
	...
}

```


[Ref = https://kernel.dk/blk-mq.pdf]
Simply put, the operating system block layer is responsible for shepherding I/O
requests from applications to storage devices. The block layer is a glue that,
on the one hand, allows applications to access diverse storage devices in a
uniform way, and on the other hand, provides storage devices and drivers with a
single point of entry from all applications. It is a convenience library to
hide the complexity and diversity of storage devices from the application while
providing common services that are valuable to applications. In addition, the
block layer implements I/O fairness, I/O error handling, I/O statistics, and
I/O scheduling that improve performance and protect end-users from malicious
device drivers.


"We also rely on stackbd to redirect page I/O requests to the disk to handle
possible remote failures and evictions."

stackbd is a virtual block device that acts as the front-end of another block
device in the Linux kernel and is used to create logical volumes or, in other
words modify I/O requests' address values and target devices. stackbd's current
implementation does not modify the requests but it acts as a sniffer and prints
for each request, whether it's a read/write, the block address, the number of
pages and the total size in bytes.

When building a block device to support paging over RDMA, we have to both offer
the page-in/page-out functionality over RDMA but we also want to use the disk
in order to handle possible remote failures and evictions. We also want to rely
on disk to asynchronously write pages or page-in/page-out from the local disk
when slabs are unmapped. However, we would like to reuse the already available
driver of the local disk instead of building everything from scratch.

The device mapper is a framework provided by the Linux kernel for mapping
physical block devices onto higher-level virtual block devices. It forms the
foundation of the LVM, software RAIDs and dm-crypt disk encryption. Device
mapper works by passing data from a virtual block device, which is provided by
the device mapper itself, to another block device. Data can also be modified in
transition, which is performed, for example, in the case of device mapper
providing disk encryption. [Ref: Wikipedia]

Why was stackbd used? To provide a way to open/read/write a block device from a
kernel module. The answer to this question is the device mapper
