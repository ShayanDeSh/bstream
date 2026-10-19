//! Kernel-side framing and route filtering for producer sockets in `SOCKHASH`.
//!
//! The stream parser splits the TCP byte stream into frames using the length in each header, so
//! the verdict runs once per whole frame. The verdict drops frames whose `route_id` is absent
//! from `ROUTES`, counting them in `DROPPED`, so unknown routes never reach user space.

#![no_std]
#![no_main]

use aya_ebpf::{
    bindings::sk_action,
    macros::{map, stream_parser, stream_verdict},
    maps::{HashMap, PerCpuArray, SockHash},
    programs::SkBuffContext,
};
use bstream_common::frame::{HEADER_LEN, PAYLOAD_LEN_OFFSET, ROUTE_ID_OFFSET};

/// Producer sockets. The key is a per-connection id chosen by user space; the value is the fd.
#[map]
static SOCKHASH: SockHash<u32> = SockHash::with_max_entries(1024, 0);

/// Route ids the router serves. Presence means pass to user space; absence means drop.
#[map]
static ROUTES: HashMap<u32, u8> = HashMap::with_max_entries(1024, 0);

/// Index 0 counts frames dropped for an unknown route, summed across CPUs by user space.
#[map]
static DROPPED: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

#[stream_parser]
fn stream_parser(ctx: SkBuffContext) -> u32 {
    // `load` fails until the whole 16-byte header has arrived; returning 0 asks for more bytes.
    match ctx.load::<u32>(PAYLOAD_LEN_OFFSET) {
        Ok(payload_len) => HEADER_LEN as u32 + u32::from_le(payload_len),
        Err(_) => 0,
    }
}

#[stream_verdict]
fn stream_verdict(ctx: SkBuffContext) -> u32 {
    let Ok(route_id) = ctx.load::<u32>(ROUTE_ID_OFFSET) else {
        // The parser only delivers whole frames, so the header is present; if a read still fails,
        // leave the decision to user space rather than dropping.
        return sk_action::SK_PASS;
    };
    if ROUTES.get_ptr(&u32::from_le(route_id)).is_some() {
        sk_action::SK_PASS
    } else {
        if let Some(dropped) = DROPPED.get_ptr_mut(0) {
            unsafe { *dropped += 1 };
        }
        sk_action::SK_DROP
    }
}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

#[unsafe(link_section = "license")]
#[unsafe(no_mangle)]
static LICENSE: [u8; 13] = *b"Dual MIT/GPL\0";
