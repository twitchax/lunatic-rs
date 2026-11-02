//! Checks that need a second node, run by `tests/distributed/run.sh` on one
//! node of a two-node cluster. Prints `distributed checks ok` when they all
//! pass, and panics otherwise. The last check prints `stop node A` and waits
//! for the script to kill the other node.

use std::time::Duration;

use lunatic::ap::handlers::{DeferredRequest, Request};
use lunatic::ap::{
    AbstractProcess, Config, DeferredRequestHandler, DeferredResponse, ProcessRef, RequestHandler,
    StartupError, State,
};
use lunatic::distributed::{node_id, nodes};
use lunatic::panic::catch_panic;
use lunatic::serializer::Bincode;
use lunatic::{Mailbox, MailboxError, MessageSignal, Process, ProcessDiedSignal, Tag};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Report {
    Value(u64),
    Started(Result<ProcessRef<Named>, ProcessRef<Named>>),
    RequestArrived,
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
    monitors_tell_nodes_apart(mailbox, remote);
    stop_monitoring_stops_reports(mailbox, remote);
    start_as_registers_once(mailbox, remote);
    requests_fail_for_missing_process(mailbox, remote);
    spawns_fail_on_unknown_node();
    messages_to_unknown_node();
    node_leaving_fails_requests_and_fires_monitors(mailbox, remote);

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

/// Process IDs repeat across nodes, so the signals name the node too.
fn monitors_tell_nodes_apart(mailbox: Monitoring, remote: u64) {
    let missing = 1_000_000;
    let remote = unsafe { Process::<()>::new(remote, missing) };
    let local = unsafe { Process::<()>::new(node_id(), missing) };
    mailbox.monitor(remote);
    assert_died(receive(mailbox), remote);
    mailbox.monitor(local);
    assert_died(receive(mailbox), local);
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

/// The script checks the panic message.
fn requests_fail_for_missing_process(mailbox: Monitoring, remote: u64) {
    let server = Server::on_node(remote).start(()).unwrap();
    assert_eq!(server.request(5), 5);
    let process = unsafe { Process::<()>::new(server.node_id(), server.id()) };
    mailbox.monitor(process);
    server.shutdown();
    assert_died(receive(mailbox), process);
    assert!(catch_panic(|| server.request(5)).is_err());
}

/// The script checks the panic messages.
fn spawns_fail_on_unknown_node() {
    let unknown = unknown_node();
    assert!(catch_panic(|| Process::spawn_node(unknown, (), |_, _: Mailbox<()>| {})).is_err());
    assert!(catch_panic(|| Named::on_node(unknown).start_as(&"unknown node", ())).is_err());
    // A failed start leaves the name free.
    assert_eq!(ProcessRef::<Named>::lookup(&"unknown node"), None);
}

/// Sends to a node that isn't in the cluster are dropped, but requests fail.
/// The script checks the panic message.
fn messages_to_unknown_node() {
    let unknown = unknown_node();
    let process = unsafe { Process::<u64>::new(unknown, 1) };
    process.send(1);
    process.tag_send(Tag::new(), 2);
    let server = unsafe { ProcessRef::<Server>::new(unknown, 1) };
    assert!(catch_panic(|| server.request(5)).is_err());
}

fn unknown_node() -> u64 {
    nodes().into_iter().max().unwrap() + 1_000
}

/// The script kills the other node in the middle of this check, and checks the
/// panic message of the failed request.
fn node_leaving_fails_requests_and_fires_monitors(mailbox: Monitoring, remote: u64) {
    let server = Server::on_node(remote).start(()).unwrap();
    let process = unsafe { Process::<()>::new(server.node_id(), server.id()) };
    mailbox.monitor(process);
    let tag = Tag::new();
    Process::spawn(
        (mailbox.this(), tag, server),
        |(parent, tag, server), _: Mailbox<()>| {
            let failed = catch_panic(|| server.deferred_request(parent)).is_err();
            parent.tag_send(tag, Report::Value(u64::from(failed)));
        },
    );
    match receive(mailbox) {
        MessageSignal::Message(Report::RequestArrived) => {}
        other => panic!("expected the request to arrive, got {other:?}"),
    }

    println!("stop node A");
    let (mut request_failed, mut died) = (false, false);
    for _ in 0..2 {
        match mailbox.receive_timeout(Duration::from_secs(30)).unwrap() {
            MessageSignal::Message(Report::Value(failed)) => {
                assert_eq!(failed, 1, "the request returned");
                request_failed = true;
            }
            MessageSignal::Signal(signal) => {
                assert_eq!(
                    signal,
                    ProcessDiedSignal {
                        node_id: remote,
                        process_id: server.id(),
                    }
                );
                died = true;
            }
            other => panic!("expected the request to fail and the server to die, got {other:?}"),
        }
    }
    assert!(request_failed && died);
    // Sends to the node that left are dropped.
    unsafe { Process::<u64>::new(remote, server.id()) }.send(1);
}

fn receive(mailbox: Monitoring) -> MessageSignal<Report, ProcessDiedSignal> {
    mailbox.receive_timeout(WAIT).unwrap()
}

fn assert_died(received: MessageSignal<Report, ProcessDiedSignal>, process: Process<()>) {
    match received {
        MessageSignal::Signal(signal) => assert_eq!(
            signal,
            ProcessDiedSignal {
                node_id: process.node_id(),
                process_id: process.id(),
            }
        ),
        other => panic!(
            "expected process {} on node {} to die, got {other:?}",
            process.id(),
            process.node_id()
        ),
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

/// Answers `u64` requests. Holds on to deferred requests without answering,
/// after telling the caller that the request arrived.
struct Server;

impl AbstractProcess for Server {
    type State = Self;
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = (Request<u64>, DeferredRequest<Process<Report>>);
    type StartupError = ();

    fn init(_: Config<Self>, _: ()) -> Result<Self, ()> {
        Ok(Self)
    }
}

impl RequestHandler<u64> for Server {
    type Response = u64;

    fn handle(_: State<Self>, request: u64) -> u64 {
        request
    }
}

impl DeferredRequestHandler<Process<Report>> for Server {
    type Response = ();

    fn handle(_: State<Self>, caller: Process<Report>, _: DeferredResponse<(), Self>) {
        caller.send(Report::RequestArrived);
    }
}
