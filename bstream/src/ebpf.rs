//! Loads the kernel-side stream parser and verdict and keeps their maps in sync with the router.
//!
//! The parser and verdict are attached to `SOCKHASH`. Every accepted producer socket is inserted
//! into `SOCKHASH` so the kernel frames and filters its stream: frames for a route in `ROUTES`
//! pass up to user space unchanged, and frames for an unknown route are dropped and counted in
//! `DROPPED` before user space ever sees them.

use std::{
    os::fd::AsRawFd,
    sync::{
        Mutex,
        atomic::{AtomicU32, Ordering},
    },
};

use anyhow::{Context as _, anyhow};
use aya::{
    Ebpf,
    maps::{HashMap, MapData, PerCpuArray, SockHash},
    programs::SkSkb,
};

/// Flags for map inserts; 0 means "create or update".
const ANY: u64 = 0;

pub struct KernelFilter {
    /// Owns the loaded programs and their links; dropping it detaches them.
    _ebpf: Ebpf,
    sockhash: Mutex<SockHash<MapData, u32>>,
    dropped: PerCpuArray<MapData, u64>,
    next_key: AtomicU32,
}

impl KernelFilter {
    /// Loads and attaches the programs and fills `ROUTES` with `route_ids`. Requires `CAP_BPF`
    /// (in practice, root).
    pub fn load(route_ids: impl IntoIterator<Item = u32>) -> anyhow::Result<Self> {
        bump_memlock_rlimit()?;
        let mut ebpf = Ebpf::load(aya::include_bytes_aligned!(concat!(
            env!("OUT_DIR"),
            "/bstream"
        )))?;

        let map_fd = {
            let sockhash: SockHash<_, u32> = ebpf
                .map("SOCKHASH")
                .context("no SOCKHASH map")?
                .try_into()?;
            sockhash.fd().try_clone()?
        };
        for name in ["stream_parser", "stream_verdict"] {
            let program: &mut SkSkb = ebpf
                .program_mut(name)
                .with_context(|| format!("no {name} program"))?
                .try_into()?;
            program.load()?;
            program.attach(&map_fd)?;
        }

        {
            let mut routes: HashMap<_, u32, u8> = ebpf
                .map_mut("ROUTES")
                .context("no ROUTES map")?
                .try_into()?;
            for route_id in route_ids {
                routes.insert(route_id, 0, ANY)?;
            }
        }

        let sockhash = ebpf.take_map("SOCKHASH").context("no SOCKHASH map")?;
        let dropped = ebpf.take_map("DROPPED").context("no DROPPED map")?;
        Ok(Self {
            _ebpf: ebpf,
            sockhash: Mutex::new(sockhash.try_into()?),
            dropped: dropped.try_into()?,
            next_key: AtomicU32::new(0),
        })
    }

    /// Hands a producer socket to the kernel programs. The key is unused by the verdict, so any
    /// value unique among live producers works.
    pub fn register_producer(&self, socket: &impl AsRawFd) -> anyhow::Result<()> {
        let key = self.next_key.fetch_add(1, Ordering::Relaxed);
        self.sockhash
            .lock()
            .map_err(|_| anyhow!("SOCKHASH mutex poisoned"))?
            .insert(key, socket.as_raw_fd(), ANY)?;
        Ok(())
    }

    /// Frames the kernel has dropped for an unknown route, summed across CPUs.
    pub fn dropped_unknown(&self) -> anyhow::Result<u64> {
        Ok(self.dropped.get(&0, ANY)?.iter().sum())
    }
}

/// eBPF maps are locked memory; on older kernels without memcg accounting the default limit is
/// too low to load the programs.
fn bump_memlock_rlimit() -> anyhow::Result<()> {
    let rlimit = libc::rlimit {
        rlim_cur: libc::RLIM_INFINITY,
        rlim_max: libc::RLIM_INFINITY,
    };
    // SAFETY: `rlimit` is a valid, fully initialized value for the duration of the call.
    if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlimit) } != 0 {
        return Err(std::io::Error::last_os_error()).context("setrlimit(RLIMIT_MEMLOCK)");
    }
    Ok(())
}
