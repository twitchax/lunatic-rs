//! Low level lunatic VM syscalls.

pub mod api;

use serde::Deserialize;

use crate::module::{params_to_vec, Param, WasmModule};
use crate::{LunaticError, ProcessConfig, Tag};

/// Performs the low level dance that will turn a high level rust function into
/// a lunatic process.
///
/// Returns the process resource ID as an u64 in case of success.
///
/// If `config` is None, the configuration used by the currently running process
/// will be inherited. If link is `Some`, the newly crated process will be
/// linked to the caller.
///
/// Only a `node` other than this one goes through the distributed API. A spawn
/// on this node is a plain local spawn, so it shares the caller's registry and
/// works without a control node.
///
/// The function `entry` will be used as entry point into the process. It will
/// be called with the argument `arg`.
pub(crate) fn spawn(
    name: Option<&str>,
    node: Option<u64>,
    config: Option<&ProcessConfig>,
    link: Option<Tag>,
    entry: fn(i32),
    arg: i32,
) -> Result<u64, SpawnError> {
    let entry = entry as usize as i32;
    let params = params_to_vec(&[Param::I32(entry), Param::I32(arg)]);
    let mut id = 0;
    let mut node_id = 0;
    let func = concat!("_lunatic_spawn_by_index_", env!("CARGO_PKG_VERSION"));
    let link = match link {
        Some(tag) => tag.id(),
        None => 0,
    };
    let config_id = config.map_or_else(|| ProcessConfig::inherit().id(), |config| config.id());
    let remote_node = node.filter(|&node| node != self::node_id());
    let result = unsafe {
        match (remote_node, name) {
            (Some(node), Some(name)) => api::distributed::get_or_spawn(
                name.as_ptr(),
                name.len(),
                node,
                config_id,
                api::distributed::module_id(),
                func.as_ptr(),
                func.len(),
                params.as_ptr(),
                params.len(),
                &mut node_id,
                &mut id,
            ),
            (Some(node), None) => api::distributed::spawn(
                node,
                config_id,
                api::distributed::module_id(),
                func.as_ptr(),
                func.len(),
                params.as_ptr(),
                params.len(),
                &mut id,
            ),
            (None, Some(name)) => api::process::get_or_spawn(
                name.as_ptr(),
                name.len(),
                link,
                config_id,
                WasmModule::inherit().id(),
                func.as_ptr(),
                func.len(),
                params.as_ptr(),
                params.len(),
                &mut node_id,
                &mut id,
            ),
            (None, None) => api::process::spawn(
                link,
                config_id,
                WasmModule::inherit().id(),
                func.as_ptr(),
                func.len(),
                params.as_ptr(),
                params.len(),
                &mut id,
            ),
        }
    };

    let error = match result {
        0 => return Ok(id),
        2 => LunaticError::NameAlreadyRegistered(node_id, id),
        _ => LunaticError::Error(id),
    };
    Err(SpawnError {
        code: result,
        error,
    })
}

/// Returned by a spawn on another node when the connection to it failed, for
/// example because it left the cluster during the spawn.
pub(crate) const SPAWN_CONNECTION_FAILED: u32 = 9027;

/// A failed [`spawn`], with the host's result code.
pub(crate) struct SpawnError {
    pub(crate) code: u32,
    pub(crate) error: LunaticError,
}

/// We attach the version to the exported function to avoid duplicate exports if
/// multiple dependencies use different versions of this crate. See:
/// https://github.com/lunatic-solutions/lunatic-rs/issues/71
#[export_name = concat!("_lunatic_spawn_by_index_", env!("CARGO_PKG_VERSION"))]
extern "C" fn _lunatic_spawn_by_index(function: i32, arg: i32) {
    let function: fn(i32) = unsafe { std::mem::transmute(function as usize) };
    function(arg);
}

pub fn process_id() -> u64 {
    unsafe { api::process::process_id() }
}

pub fn node_id() -> u64 {
    unsafe { api::distributed::node_id() }
}

/// Sends the message in the scratch area. A message to a process that doesn't
/// exist, or whose node isn't in the cluster, is dropped.
pub fn send(node: u64, process_id: u64) {
    if node_id() == node {
        unsafe { api::message::send(process_id) }
    } else {
        unsafe { api::distributed::send(node, process_id) }
    };
}

pub fn monitor(node: u64, process_id: u64) {
    if node_id() == node {
        unsafe { api::process::monitor(process_id) }
    } else {
        unsafe { api::distributed::monitor(node, process_id) }
    };
}

pub fn stop_monitoring(node: u64, process_id: u64) {
    if node_id() == node {
        unsafe { api::process::stop_monitoring(process_id) }
    } else {
        unsafe { api::distributed::stop_monitoring(node, process_id) }
    };
}

pub fn send_receive_skip_search(node: u64, process_id: u64, wait_on_tag: i64, timeout: u64) -> u32 {
    if node_id() == node {
        unsafe { api::message::send_receive_skip_search(process_id, wait_on_tag, timeout) }
    } else {
        unsafe {
            api::distributed::send_receive_skip_search(node, process_id, wait_on_tag, timeout)
        }
    }
}

/// Utility for calling an allocating host function which is deserialized into
/// `T`.
///
/// # Example
///
/// ```no_run
/// use lunatic::host::call_host_alloc;
/// # unsafe fn some_allocating_fn(_len_ptr: *mut u32) -> u32 { unimplemented!() }
///
/// #[derive(serde::Deserialize)]
/// struct Foo { a: String }
///
/// let foo = call_host_alloc::<Foo>(|len_ptr| unsafe {
///     some_allocating_fn(len_ptr)
/// }).unwrap();
/// ```
#[doc(hidden)]
pub fn call_host_alloc<T>(f: impl Fn(*mut u32) -> u32) -> bincode::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let mut len = 0_u32;
    let len_ptr = &mut len as *mut u32;
    let ptr = f(len_ptr);
    let data_vec = unsafe { Vec::from_raw_parts(ptr as *mut u8, len as usize, len as usize) };
    bincode::deserialize(&data_vec)
}
