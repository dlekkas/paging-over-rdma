#include <linux/module.h>
#include <linux/frontswap.h>
#include <linux/vmalloc.h>
#include <linux/page-flags.h>
#include <linux/memcontrol.h>
#include <linux/smp.h>

#include <linux/init.h>

static void swapmon_init(unsigned swap_type) {
	printk("swapmon_init(%d)", swap_type);
}

static int swapmon_store(unsigned swap_type, pgoff_t offset,
												 struct page *page) {
	//printk("swapmon: store(swap_type = %-10u, offset = %lu)\n",
				 //swap_type, offset);
	return -1;
}

static int swapmon_load(unsigned swap_type, pgoff_t offset,
		struct page *page) {
	printk("swapmon: load(swap_type = %-10u, offset = %lu)\n",
				 swap_type, offset);
	return -1;
}

static void swapmon_invalidate_page(unsigned swap_type, pgoff_t offset) {
	printk("swapmon: invalidate_page(swap_type = %-10u, offset = %lu)\n",
				 swap_type, offset);
}

static void swapmon_invalidate_area(unsigned swap_type) {
	printk("swapmon: invalidate_area(swap_type = %-10u)\n", swap_type);
}

static struct frontswap_ops swapmon_fs_ops = {
	.init  = swapmon_init,
	.store = swapmon_store,
	.load  = swapmon_load,
	.invalidate_page = swapmon_invalidate_page,
	.invalidate_area = swapmon_invalidate_area,
};

static int __init swapmonfs_init(void) {
	frontswap_register_ops(&swapmon_fs_ops);
	printk("swapmon: registered frontswap ops\n");
	printk("swapmon: module loaded\n");
	return 0;
}

static void __exit swapmonfs_exit(void) {
	printk("swapmon: module unloaded\n");
}

module_init(swapmonfs_init);
module_exit(swapmonfs_exit);
