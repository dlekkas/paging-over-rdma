# Frontswap Background

Frontswap is entirely synchronous while a swap device is asynchronous and uses
block I/O. The block I/O layer may perform "optimizations" that are not suitable
for a RAM-like device (e.g. such as remote memory), including delaying the write
of some pages for a significant amount of time. Synchrony is required to ensure
the dynamicity of the backend and to avoid thorny race conditions that would
unnecessarily and greatly complicate frontswap and/or the block I/O subsystem.
Only the initial "store" and "load" operations need to be synchronous.

There is a downside to tmem specifications for frontswap: Since any "store"
might fail, there must always be a real slot on a real swap device to swap
the page. Thus frontswap must be implemented as a "shadow" to every swapon'd
device with the potential capability of holding every page that the swap
device might have held and the possibility that it might hold no pages at all.

There is a peculiarity with frontswap regarding duplicate stores. If a page
has been successfully stored, it may not be successfully overwritten. Nearly
always it can, but sometimes it cannot. Whenever frontswap rejects a store that
would overwrite, it also must invalidate the old data and ensure that it is no
longer accessible. The swap subsystem then writes the new data to the real swap
device which is the correct course of action to ensure coherency.

When the swap subsystem swaps out a page to a real swap device, that page is
only taking up low-value pre-allocated disk space. But if frontswap has placed
a page in tmem, then that page may be taking up valuable real estate. The
`frontswap_shrink` routine allows code outside of the swap subsystem to force
pages out of the memory managed by frontswap and back into kernel-addressable
memory.

Source: [here](https://www.kernel.org/doc/html/latest/vm/frontswap.html)
