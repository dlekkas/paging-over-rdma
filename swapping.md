## Swap space
Once a Linux system starts running low on available page frames, it needs a way
to select old pages to invalidate and make them available for subsequent needs.
Our primary interest is not on the page replacement policy but rather on what
happens when a page needs to be replaced. We also want to focus on the
interaction between the swap device and the page replacement daemon.

## Swap kernel impl
During system startup, the module *kswapd* is loaded and calls its init
function called `kswapd_init()` which subsequently invokes `kswapd_run()` which
starts kernel thread called *kswapd*. The thread invokes the function
`static int kswapd(void *p)` which is represents the background pageout daemon.
This daemon basically trickles out pages so that we have _some_ free memory
available even if there is no other activity that frees anything up. *kswapd*
sleeps for most of the time and once it's waken up, it cycles through all mem
zones checking whether pages need to be swapped out and if that's the case then
it invokes `do_try_to_free_pages(struct zonelist*, struct scan_control*)` which
returns the number of reclaimed pages. If a full scan on the inactive list of
those zones fails to free enough memory, then we are OOM and something needs to
be killed.

The above func invokes `shrink_zones(struct zonelist*, struct scan_control*)`
which represents the direct reclaim path, for page-allocating processes. The
`shrink_page_list()` func invoked by `shrink_zones()` allocates swap space by
invoking `add_to_swap()`. After allocating the space in swap, the page is
checked for all mappings into virtual memory of processes, unmapped with
`try_to_unmap()` and checked for writeback (e.g. dirty page).

Finally, `pageout(struct page *, struct address_space *, struct scan_control *)`
is called by `shrink_page_list()` for each dirty page. This consequently invokes
the function in field `mapping->a_ops->writepage` where the mapping represents
the `address_space` struct.

## What is the `address_space` object?
A page in the page cache can consist of multiple noncontiguous physical disk
blocks. The object `address_space` is used to manage entries in the cache and
page I/O operations. It could also be seen as the physical analogue to the
virtual `vm_area_struct` where a single file may be represented by multiple
`vm_area_struct` structures if it is `mmap()`ed by multiple processes, but it
exists only once in physical memory. Definitions in `<linux/fs.h>`:
```
struct address_space {
	struct inode *host; // owner: inode, block_device
	...
	const struct address_space_operations *a_ops;
	...
};
```
The `a_ops` field points to the address space operations table, in the same
manner as the VFS objects and their operations tables.

```
struct address_space_operations {
	int (*writepage)(struct page *page, struct writeback_control *wbc);
	...
	int (*swap_activate)(struct swap_info_struct *sis, struct file *file,
											 sector_t *span);
	int (*swap_deactivate)(struct file *file);
}
```
These function pointers point at the functions that implement page I/O for this
cached object. Each backing store describes how it interacts with the page cache
via its own `address_space_operations`. For example, the ext4 filesystem defines
its operations in `fs/ext4/inode.c` and the `writepage` field of its
`address_space_operations` points to `ext4_writepage()`. Afterwards, the
function `ext4_io_submit()` is invoked which invokes `submit_bio` which submits
a BIO to the block device layer for I/O by adding it to the request queue.

# Does swap device have a file system?
Swap is an option when formatting a drive, but isn't an actual file system. It's
used as virtual memory and doesn't have a file system structure. The whole
purpose of a filesystem is to structure data in certain way. Swap partition in
particular doesn't have structure, but it does have a specific header, which is
created by `mkswap` program.

Dirty pages are pages which can be swapped out. Linux uses an architecture
specific bit in the PTE to describe pages this way. However, not all dirty
pages are necessarily written to the swap file. Every virtual memory region of
a process may have its own swap operation (pointed at by the `vm_ops` pointer
in the `vm_area_struct`) and that method is used. Otherwise, the swap daemon
will allocate a page in the swap file and write the page out to that device.

The page's page table entry is replaced by one which is marked as invalid but
which contains information about where the page is in the swap file. This is an
offset into the swap file where the page is held and an indication of which swap
file is being used.

[Ref = https://tldp.org/LDP/tlk/mm/memory.html]

This functionality is implemented in
`mm/vmscan.c`.


# What is a page cache?
The page cache represents a cache of pages in RAM. The pages originate from
reads and writes of regular filesystem files, block device files, and memory
mapped files. In this manner, the page cache contains chunks of recently
accessed files. During a page I/O operation, such as `read()`, the kernel checks
whether the data resides in the page cache. If the data is in the page cache,
the kernel can quickly return the requested page from memory rather than read
the data off the comparatively slow disk.

All data that is read from disk is stored in the page cache to reduce the
amount of disk IO that must be performed.
