//! Checks that need a second node, run by `tests/distributed/run.sh` on one
//! node of a two-node cluster. Prints `distributed checks ok` when they all
//! pass, and panics otherwise.

use std::time::Duration;

use lunatic::ap::{AbstractProcess, Config, ProcessRef, StartupError};
use lunatic::distributed::{node_id, nodes};
use lunatic::serializer::Bincode;
use lunatic::{Mailbox, MailboxError, MessageSignal, Process, ProcessDiedSignal, Tag};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Report {
    Value(u64),
    Started(Result<ProcessRef<Named>, ProcessRef<Named>>),
}

type Monitoring = Mailbox<Report, Bincode, ProcessDiedSignal>;

#[lunatic::main]
fn main(mailbox: Mailbox<Report>) {
    let remote = nodes()
        .into_iter()
        .find(|&node| node != node_id())
        .expect("no other node");
    let mailbox = mailbox.monitorable();

    let gone = monitor_fires_after_normal_exit(mailbox, remote);
    monitor_fires_for_missing_process(mailbox, gone);
    stop_monitoring_stops_reports(mailbox, remote);
    start_as_registers_once(mailbox, remote);

    println!("distributed checks ok");
}

/// Returns the exited process.
fn monitor_fires_after_normal_exit(mailbox: Monitoring, remote: u64) -> Process<()> {
    // The child exits on its first message, after the monitor is in place.
    let child = Process::spawn_node(remote, mailbox.this(), |parent, child: Mailbox<()>| {
        child.receive();
        parent.send(Report::Value(42));
    });
    mailbox.monitor(child);
    child.send(());
    match receive(mailbox) {
        MessageSignal::Message(Report::Value(42)) => {}
        other => panic!("expected the child's message first, got {other:?}"),
    }
    assert_died(receive(mailbox), child);
    child
}

fn monitor_fires_for_missing_process(mailbox: Monitoring, gone: Process<()>) {
    mailbox.monitor(gone);
    assert_died(receive(mailbox), gone);
}

fn stop_monitoring_stops_reports(mailbox: Monitoring, remote: u64) {
    let child = Process::spawn_node(remote, (), |_, child: Mailbox<()>| child.receive());
    mailbox.monitor(child);
    mailbox.stop_monitoring(child);
    child.send(());
    assert!(matches!(
        mailbox.receive_timeout(Duration::from_secs(2)),
        Err(MailboxError::TimedOut)
    ));
}

/// Several processes race to start the same name on the other node: exactly
/// one starts it, and the others get that process back.
fn start_as_registers_once(mailbox: Monitoring, remote: u64) {
    const RACERS: usize = 8;
    let tag = Tag::new();
    for _ in 0..RACERS {
        Process::spawn(
            (mailbox.this(), tag, remote),
            |(parent, tag, remote), _: Mailbox<()>| {
                let started = match Named::on_node(remote).start_as(&"distributed", ()) {
                    Ok(named) => Ok(named),
                    Err(StartupError::NameAlreadyRegistered(named)) => Err(named),
                    Err(err) => panic!("start_as failed: {err:?}"),
                };
                parent.tag_send(tag, Report::Started(started));
            },
        );
    }

    let mut winner = None;
    let mut others = Vec::new();
    for _ in 0..RACERS {
        match mailbox.tag_receive_timeout(&[tag], WAIT) {
            Ok(MessageSignal::Message(Report::Started(Ok(named)))) => {
                assert!(
                    winner.replace(named).is_none(),
                    "two processes got the name"
                );
            }
            Ok(MessageSignal::Message(Report::Started(Err(named)))) => others.push(named),
            other => panic!("expected a start_as result, got {other:?}"),
        }
    }
    let winner = winner.expect("nobody got the name");
    assert_eq!(winner.node_id(), remote);
    assert!(others.iter().all(|named| *named == winner));
    assert_eq!(ProcessRef::<Named>::lookup(&"distributed"), Some(winner));
}

fn receive(mailbox: Monitoring) -> MessageSignal<Report, ProcessDiedSignal> {
    mailbox.receive_timeout(WAIT).unwrap()
}

fn assert_died(received: MessageSignal<Report, ProcessDiedSignal>, process: Process<()>) {
    match received {
        MessageSignal::Signal(ProcessDiedSignal(id)) => assert_eq!(id, process.id()),
        other => panic!("expected process {} to die, got {other:?}", process.id()),
    }
}

struct Named;

impl AbstractProcess for Named {
    type State = Self;
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = ();
    type StartupError = ();

    fn init(_: Config<Self>, _: ()) -> Result<Self, ()> {
        Ok(Self)
    }
}
