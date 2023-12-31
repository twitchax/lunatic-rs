use std::marker::PhantomData;

use super::{lifecycles, AbstractProcess, ProcessRef, StartupError};
use crate::{function::process::{process_name, ProcessType}, MailboxError};
use crate::{LunaticError, Mailbox, Process, ProcessConfig, ProcessName, Tag};

trait IntoAbstractProcessBuilder<T> {}

/// Holds additional information about [`AbstractProcess`] spawning.
///
/// This information can include data about the process configuration, what node
/// it should be spawned on, if the process should be linked and with which tag.
///
/// It implements the same public interface as [`AbstractProcess`], so that the
/// builder pattern can start with the [`AbstractProcess`] and transition to the
/// [`AbstractProcessBuilder`].
pub struct AbstractProcessBuilder<'a, T: ?Sized> {
    link: Option<Tag>,
    config: Option<&'a ProcessConfig>,
    node: Option<u64>,
    phantom: PhantomData<T>,
}

impl<'a, T> AbstractProcessBuilder<'a, T>
where
    T: AbstractProcess,
{
    pub(crate) fn new() -> AbstractProcessBuilder<'a, T> {
        AbstractProcessBuilder {
            link: None,
            config: None,
            node: None,
            phantom: PhantomData,
        }
    }

    /// Links the to be spawned process to the parent.
    pub fn link(self) -> AbstractProcessBuilder<'a, T> {
        AbstractProcessBuilder {
            link: Some(Tag::new()),
            config: self.config,
            node: self.node,
            phantom: PhantomData,
        }
    }

    /// Links the to be spawned process to the parent with a specific [`Tag`].
    pub fn link_with(self, tag: Tag) -> AbstractProcessBuilder<'a, T> {
        AbstractProcessBuilder {
            link: Some(tag),
            config: self.config,
            node: self.node,
            phantom: PhantomData,
        }
    }

    /// Allows for spawning the process with a specific configuration.
    pub fn configure(self, config: &'a ProcessConfig) -> AbstractProcessBuilder<'a, T> {
        AbstractProcessBuilder {
            link: self.link,
            config: Some(config),
            node: self.node,
            phantom: PhantomData,
        }
    }

    /// Sets the node on which the process will be spawned.
    pub fn on_node(self, node: u64) -> AbstractProcessBuilder<'a, T> {
        AbstractProcessBuilder {
            link: self.link,
            config: self.config,
            node: Some(node),
            phantom: PhantomData,
        }
    }

    /// Starts a new `AbstractProcess` and returns a reference to it.
    ///
    /// This call will block until the `init` function finishes. If the `init`
    /// function returns an error, it will be returned as
    /// `StartupError::Custom(error)`. If the `init` function panics during
    /// execution, it will return [`StartupError::InitPanicked`].
    #[track_caller]
    pub fn start(&self, arg: T::Arg) -> Result<ProcessRef<T>, StartupError<T>> {
        let init_tag = Tag::new();
        let this = unsafe { Process::<Result<(), StartupError<T>>, T::Serializer>::this() };
        let entry_data = (this, init_tag, arg);
        let process = match (self.link, &self.config, self.node) {
            (Some(_), _, Some(_node)) => {
                unimplemented!("Linking across nodes is not supported yet");
            }
            (Some(tag), Some(config), None) => Process::<(), T::Serializer>::spawn_link_config_tag(
                config,
                entry_data,
                tag,
                lifecycles::entry::<T>,
            ),
            (Some(tag), None, None) => Process::<(), T::Serializer>::spawn_link_tag(
                entry_data,
                tag,
                lifecycles::entry::<T>,
            ),
            (None, Some(config), Some(node)) => Process::<(), T::Serializer>::spawn_node_config(
                node,
                config,
                entry_data,
                lifecycles::entry::<T>,
            ),
            (None, None, Some(node)) => {
                Process::<(), T::Serializer>::spawn_node(node, entry_data, lifecycles::entry::<T>)
            }
            (None, Some(config), None) => Process::<(), T::Serializer>::spawn_config(
                config,
                entry_data,
                lifecycles::entry::<T>,
            ),
            (None, None, None) => {
                Process::<(), T::Serializer>::spawn(entry_data, lifecycles::entry::<T>)
            }
        };

        // Wait on `init()`
        let mailbox: Mailbox<Result<(), StartupError<T>>, T::Serializer> =
            unsafe { Mailbox::new() };
        match mailbox.tag_receive(&[init_tag]) {
            Ok(()) => Ok(ProcessRef { process }),
            Err(err) => Err(err),
        }
    }

    /// Starts a new `AbstractProcess` and returns a reference to it.
    ///
    /// This call will block until the `init` function finishes. If the `init`
    /// function returns an error, it will be returned as
    /// `StartupError::Custom(error)`. If the `init` function panics during
    /// execution, it will return [`StartupError::InitPanicked`].
    #[track_caller]
    pub fn start_timeout(&self, arg: T::Arg, timeout: std::time::Duration) -> Result<ProcessRef<T>, StartupError<T>> {
        let init_tag = Tag::new();
        let this = unsafe { Process::<Result<(), StartupError<T>>, T::Serializer>::this() };
        let entry_data = (this, init_tag, arg);
        let process = match (self.link, &self.config, self.node) {
            (Some(_), _, Some(_node)) => {
                unimplemented!("Linking across nodes is not supported yet");
            }
            (Some(tag), Some(config), None) => Process::<(), T::Serializer>::spawn_link_config_tag(
                config,
                entry_data,
                tag,
                lifecycles::entry::<T>,
            ),
            (Some(tag), None, None) => Process::<(), T::Serializer>::spawn_link_tag(
                entry_data,
                tag,
                lifecycles::entry::<T>,
            ),
            (None, Some(config), Some(node)) => Process::<(), T::Serializer>::spawn_node_config(
                node,
                config,
                entry_data,
                lifecycles::entry::<T>,
            ),
            (None, None, Some(node)) => {
                Process::<(), T::Serializer>::spawn_node(node, entry_data, lifecycles::entry::<T>)
            }
            (None, Some(config), None) => Process::<(), T::Serializer>::spawn_config(
                config,
                entry_data,
                lifecycles::entry::<T>,
            ),
            (None, None, None) => {
                Process::<(), T::Serializer>::spawn(entry_data, lifecycles::entry::<T>)
            }
        };

        // Wait on `init()`
        let mailbox: Mailbox<Result<(), StartupError<T>>, T::Serializer> =
            unsafe { Mailbox::new() };
        match mailbox.tag_receive_timeout(&[init_tag], timeout) {
            Ok(m) => match m {
                Ok(()) => Ok(ProcessRef { process }),
                Err(err) => Err(err),
            },
            Err(err) => match err {
                MailboxError::TimedOut => Err(StartupError::TimedOut),
                _ => unreachable!("tag_receive_timeout should panic in case of other errors"),
            },
        }
    }

    /// Starts the process and registers it under `name`. If another process is
    /// already registered under the same name, it will return a
    /// `Err(StartupError::NameAlreadyRegistered(proc))` with a reference to the
    /// existing process.
    ///
    /// This call will block until the `init` function finishes. If the `init`
    /// function returns an error, it will be returned as
    /// `StartupError::Custom(error)`. If the `init` function panics during
    /// execution, it will return [`StartupError::InitPanicked`].
    ///
    /// If used in combination with the [`on_node`](Self::on_node) option, the
    /// name registration will be performed on the local node and not the remote
    /// one.
    #[track_caller]
    pub fn start_as<N: ProcessName>(
        &self,
        name: &N,
        arg: T::Arg,
    ) -> Result<ProcessRef<T>, StartupError<T>> {
        let name: &str = name.process_name();
        let name = process_name::<T, T::Serializer>(ProcessType::ProcessRef, name);
        let init_tag = Tag::new();
        let this = unsafe { Process::<Result<(), StartupError<T>>, T::Serializer>::this() };
        let entry_data = (this, init_tag, arg);
        let process = match (self.link, &self.config, self.node) {
            (Some(_), _, Some(_node)) => {
                unimplemented!("Linking across nodes is not supported yet");
            }
            (Some(tag), Some(config), None) => {
                Process::<(), T::Serializer>::name_spawn_link_config_tag(
                    &name,
                    config,
                    entry_data,
                    tag,
                    lifecycles::entry::<T>,
                )
            }
            (Some(tag), None, None) => Process::<(), T::Serializer>::name_spawn_link_tag(
                &name,
                entry_data,
                tag,
                lifecycles::entry::<T>,
            ),
            (None, Some(config), Some(node)) => {
                Process::<(), T::Serializer>::name_spawn_node_config(
                    &name,
                    node,
                    config,
                    entry_data,
                    lifecycles::entry::<T>,
                )
            }
            (None, None, Some(node)) => Process::<(), T::Serializer>::name_spawn_node(
                &name,
                node,
                entry_data,
                lifecycles::entry::<T>,
            ),
            (None, Some(config), None) => Process::<(), T::Serializer>::name_spawn_config(
                &name,
                config,
                entry_data,
                lifecycles::entry::<T>,
            ),
            (None, None, None) => {
                Process::<(), T::Serializer>::name_spawn(&name, entry_data, lifecycles::entry::<T>)
            }
        };

        let process = match process {
            Ok(process) => process,
            Err(LunaticError::NameAlreadyRegistered(node_id, process_id)) => {
                // If a process under this name already exists, return it.
                return Err(StartupError::NameAlreadyRegistered(ProcessRef {
                    process: unsafe { Process::new(node_id, process_id) },
                }));
            }
            _ => unreachable!(),
        };

        // Wait on `init()`
        let mailbox: Mailbox<Result<(), StartupError<T>>, T::Serializer> =
            unsafe { Mailbox::new() };
        match mailbox.tag_receive(&[init_tag]) {
            Ok(()) => Ok(ProcessRef { process }),
            Err(err) => Err(err),
        }
    }
}
