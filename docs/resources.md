## A curated list of awesome thesis-related readings

### Linux I/O subsystem
- Linux Block IO present and future - paper written by Jens Axboe describing linux block layer. [here](https://www.landley.net/kdocs/ols/2004/ols2004v1-pages-51-62.pdf)
- Excellent block I/O layer blog series (four parts). [here](https://ari-ava.blogspot.com/2014/06/opw-linux-block-io-layer-part-1-base.html)

### `blk-mq`
- [SYSTOR '13'] Linux Block IO: Introducing Multi-queue SSD Access on Multi-core Systems - paper introducing `blk-mq` written by the kernel hacker and author of `blk-mq` Jens Axboe. [here](https://kernel.dk/systor13-final18.pdf)
- `blk-mq`: new multi-queue block IO queuing mechanism - git commit introducing `blk-mq` and highlights some of its helper functions. [here](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=320ae51feed5c2f13664aa05a76bec198967e04d)
- The multiqueue block layer - introduction. [here](https://lwn.net/Articles/552904/)
- Kernel man docs for `blk-mq` for kernel 5.10. [here](https://www.kernel.org/doc/html/v5.10/block/blk-mq.html)
- Simple block device tutorial under `blk-mq` for Kernel 5.0 [here](https://prog.world/linux-kernel-5-0-we-write-simple-block-device-under-blk-mq/)
- Short API walkthrough for `blk-mq`. [here](https://hyunyoung2.github.io/2016/09/14/Multi_Queue/)
