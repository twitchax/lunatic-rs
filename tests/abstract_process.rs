use std::time::Duration;

use lunatic::ap::handlers::{DeferredRequest, Message, Request};
use lunatic::ap::{
    AbstractProcess, Config, DeferredRequestHandler, DeferredResponse, MessageHandler, ProcessRef,
    RequestHandler, StartupError, State,
};
use lunatic::serializer::Bincode;
use lunatic::time::Timeout;
use lunatic::{
    sleep, spawn_link, test, LinkDiedSignal, Mailbox, MailboxError, MessageSignal, Process,
    ProcessConfig, Tag,
};

/// This `AbstractProcess` always panics on `init`.
struct InitPanicksAP;

impl AbstractProcess for InitPanicksAP {
    type State = ();
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = ();
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<(), ()> {
        panic!("Startup failed");
    }
}

#[test]
fn init_failure() {
    assert_eq!(InitPanicksAP::start(()), Err(StartupError::InitPanicked));
}

/// This `AbstractProcess` returns an error on `init`.
struct InitErrorAP;

impl AbstractProcess for InitErrorAP {
    type State = ();
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = ();
    type StartupError = String;

    fn init(_: Config<Self>, _: Self::Arg) -> Result<(), String> {
        Err("Failed".to_owned())
    }
}

#[test]
fn init_error() {
    assert_eq!(
        InitErrorAP::start(()),
        Err(StartupError::Custom("Failed".to_owned()))
    );
}

/// `AbstractProcess` that starts normally.
struct InitOkAP;

impl AbstractProcess for InitOkAP {
    type State = ();
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = ();
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<(), ()> {
        Ok(())
    }
}

#[test]
fn init_ok() {
    assert!(InitOkAP::start(()).is_ok());
}

#[test]
fn shutdown_ok() {
    let ap = InitOkAP::start(()).unwrap();
    ap.shutdown();
}

/// `AbstractProcess` that fails to shut down in time.
struct ShutdownTimeoutAP;

impl AbstractProcess for ShutdownTimeoutAP {
    type State = ();
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = ();
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<(), ()> {
        Ok(())
    }

    fn terminate(_state: Self::State) {
        sleep(Duration::from_millis(100));
    }
}

#[test]
fn shutdown_timeout() {
    let ap = ShutdownTimeoutAP::start(()).unwrap();
    assert!(ap
        .with_timeout(Duration::from_millis(10))
        .shutdown()
        .is_err());
}

/// `AbstractProcess` with float array as `init` arguments.
struct FloatsServerAP(Vec<f64>);

impl AbstractProcess for FloatsServerAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = Vec<f64>;
    type Handlers = (Message<Add>, Request<Sum>);
    type StartupError = ();

    fn init(_: Config<Self>, arg: Self::Arg) -> Result<Self, ()> {
        Ok(Self(arg))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Add(f64);
impl MessageHandler<Add> for FloatsServerAP {
    fn handle(mut state: State<Self>, add: Add) {
        state.0.push(add.0);
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Sum;
impl RequestHandler<Sum> for FloatsServerAP {
    type Response = f64;

    fn handle(state: State<Self>, _: Sum) -> Self::Response {
        state.0.iter().sum()
    }
}

#[test]
fn float_message_and_request_handling() {
    let init = vec![0.1, 0.1, 0.1, 0.2];
    let ap = FloatsServerAP::link().start(init).unwrap();

    ap.send(Add(0.2));
    ap.send(Add(0.2));
    ap.send(Add(0.1));
    ap.send(Add(1.0));
    assert_eq!(ap.request(Sum), 2.0);
    ap.send(Add(0.1));
    assert_eq!(ap.request(Sum), 2.1);
    ap.send(Add(0.1));
    assert_eq!(ap.request(Sum), 2.2);
    ap.send(Add(0.3));
    assert_eq!(ap.request(Sum), 2.5);
    ap.send(Add(0.1));
    assert_eq!(ap.request(Sum), 2.6);
}

/// `AbstractProcess` that self-references itself during `init` and in handlers.
struct SelfRefAP(u32);

impl AbstractProcess for SelfRefAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = u32;
    type Handlers = (Message<Inc>, Request<Count>);
    type StartupError = ();

    fn init(config: Config<Self>, start: Self::Arg) -> Result<Self, ()> {
        // Send increment message before constructing state.
        config.self_ref().send(Inc);
        Ok(Self(start))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Inc;
impl MessageHandler<Inc> for SelfRefAP {
    fn handle(mut state: State<Self>, _: Inc) {
        // Increment state until 10
        if state.0 < 10 {
            state.0 += 1;
            // Increment state again
            state.self_ref().send(Inc);
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Count;
impl RequestHandler<Count> for SelfRefAP {
    type Response = u32;

    fn handle(state: State<Self>, _: Count) -> Self::Response {
        state.0
    }
}

#[test]
fn self_ref() {
    let ap = SelfRefAP::link().start(0).unwrap();
    // Give enough time to increment state.
    sleep(Duration::from_millis(20));
    assert_eq!(ap.request(Count), 10);
}

/// `AbstractProcess` that is registered under a well-known name.
struct RegisteredAP;

impl AbstractProcess for RegisteredAP {
    type State = ();
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = ();
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<(), ()> {
        // Doing a lookup in `init` should not deadlock.
        let _ = ProcessRef::<InitOkAP>::lookup(&"_");
        Ok(())
    }
}

#[test]
fn lookup() {
    let ap = RegisteredAP::start_as(&"AP", ()).unwrap();
    let lookup = ProcessRef::<RegisteredAP>::lookup(&"AP").unwrap();
    assert_eq!(ap, lookup);
    let exists = RegisteredAP::start_as(&"AP", ());
    assert_eq!(exists, Err(StartupError::NameAlreadyRegistered(ap)));
    // Registering a different process type under the same name will work.
    let doesnt_exist = InitOkAP::start_as(&"AP", ());
    assert!(doesnt_exist.is_ok());
}

#[test]
fn start_as_on_this_node_registers_the_name() {
    let node = lunatic::host::node_id();
    let ap = RegisteredAP::on_node(node).start_as(&"AP", ()).unwrap();
    assert_eq!(ProcessRef::<RegisteredAP>::lookup(&"AP"), Some(ap));
    let exists = RegisteredAP::on_node(node).start_as(&"AP", ());
    assert_eq!(exists, Err(StartupError::NameAlreadyRegistered(ap)));
}

// Without a control node a remote spawn traps, so this also shows the name is
// checked before anything is spawned.
#[test]
fn start_as_on_remote_node_checks_the_name_first() {
    let ap = RegisteredAP::start_as(&"AP", ()).unwrap();
    let remote = lunatic::host::node_id() + 1;
    let exists = RegisteredAP::on_node(remote).start_as(&"AP", ());
    assert_eq!(exists, Err(StartupError::NameAlreadyRegistered(ap)));
}

/// `AbstractProcess` that can panic on message.
struct PanicOnMessageAP;

impl AbstractProcess for PanicOnMessageAP {
    type State = ();
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = (Message<Panick>,);
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<(), ()> {
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Panick;

impl MessageHandler<Panick> for PanicOnMessageAP {
    fn handle(_: State<Self>, _: Panick) {
        panic!();
    }
}

#[test]
#[should_panic]
fn linked_process_fails() {
    let ap = PanicOnMessageAP::start(()).unwrap();
    ap.link();
    ap.send(Panick);
    sleep(Duration::from_millis(10));
}

#[test]
fn unlinked_process_doesnt_fail() {
    let ap = PanicOnMessageAP::link().start(()).unwrap();
    ap.unlink();
    ap.send(Panick);
    sleep(Duration::from_millis(10));
}

#[test]
fn linked_start_on_this_node_reports_failure(mailbox: Mailbox<()>) {
    let mailbox = mailbox.catch_link_failure();
    let tag = Tag::new();
    let ap = PanicOnMessageAP::link_with(tag)
        .on_node(lunatic::host::node_id())
        .start(())
        .unwrap();
    ap.send(Panick);
    match mailbox.receive_timeout(Duration::from_secs(1)) {
        Ok(MessageSignal::Signal(LinkDiedSignal(died))) => assert_eq!(died, tag),
        other => panic!("expected the link to report the failure, got {other:?}"),
    }
}

#[test]
#[should_panic(expected = "a linked start is not supported for remote processes")]
fn linked_start_on_remote_node_is_refused() {
    let _ = PanicOnMessageAP::link()
        .on_node(lunatic::host::node_id() + 1)
        .start(());
}

/// `AbstractProcess` that handles failed links
struct HandleLinkPanicAP {
    panicked: bool,
}

impl AbstractProcess for HandleLinkPanicAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = (Request<DidPanick>,);
    type StartupError = ();

    fn init(config: Config<Self>, _: Self::Arg) -> Result<Self, ()> {
        config.die_if_link_dies(false);
        spawn_link!(|| panic!());
        Ok(Self { panicked: false })
    }

    fn handle_link_death(mut state: State<Self>, tag: lunatic::Tag) {
        println!("Link trapped: {:?}", tag);
        state.panicked = true;
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DidPanick;

impl RequestHandler<DidPanick> for HandleLinkPanicAP {
    type Response = bool;

    fn handle(state: State<Self>, _: DidPanick) -> Self::Response {
        state.panicked
    }
}

#[test]
fn handle_link_panic() {
    let ap = HandleLinkPanicAP::start(()).unwrap();
    sleep(Duration::from_millis(10));
    assert!(ap.request(DidPanick));
}

/// `AbstractProcess` that handles `String` message
struct StringHandlerAP;

impl AbstractProcess for StringHandlerAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = (Message<String>,);
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<Self, ()> {
        Ok(Self)
    }
}

impl MessageHandler<String> for StringHandlerAP {
    fn handle(_: State<Self>, message: String) {
        println!("what");
        assert_eq!(message, "Hello process");
    }
}

#[test]
fn handle_message() {
    let ap = StringHandlerAP::link().start(()).unwrap();
    ap.send("Hello process".to_owned());
    sleep(Duration::from_millis(10));
}

/// `AbstractProcess` that handles a `String` request/response
struct StringRequestHandlerAP;

impl AbstractProcess for StringRequestHandlerAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = (Request<String>,);
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<Self, ()> {
        Ok(Self)
    }
}

impl RequestHandler<String> for StringRequestHandlerAP {
    type Response = String;

    fn handle(_: State<Self>, mut request: String) -> Self::Response {
        request.push_str(" world");
        request
    }
}

#[test]
fn handle_request() {
    let ap = StringRequestHandlerAP::link().start(()).unwrap();
    let response = ap.request("Hello".to_owned());
    assert_eq!(response, "Hello world");
}

/// `AbstractProcess` that times out on requests
struct RequestHandlerTimeoutAP;

impl AbstractProcess for RequestHandlerTimeoutAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = (Request<()>,);
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<Self, ()> {
        Ok(Self)
    }
}

impl RequestHandler<()> for RequestHandlerTimeoutAP {
    type Response = ();

    fn handle(_: State<Self>, _: ()) -> Self::Response {
        sleep(Duration::from_millis(25));
    }
}

#[test]
fn request_timeout() {
    let ap = RequestHandlerTimeoutAP::link().start(()).unwrap();
    let response = ap.with_timeout(Duration::from_millis(10)).request(());
    assert_eq!(response, Err(Timeout));
}

/// `AbstractProcess` that handles a deferred `String` request/response
struct DeferredStringRequestHandlerAP;

impl AbstractProcess for DeferredStringRequestHandlerAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = (DeferredRequest<String>,);
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<Self, ()> {
        Ok(Self)
    }
}

impl DeferredRequestHandler<String> for DeferredStringRequestHandlerAP {
    type Response = String;

    fn handle(
        _: State<Self>,
        request: String,
        deferred_response: DeferredResponse<Self::Response, Self>,
    ) {
        spawn_link!(|request, deferred_response| {
            request.push_str(" world");
            deferred_response.send_response(request);
        });
    }
}

#[test]
fn deferred_handle_request() {
    let ap = DeferredStringRequestHandlerAP::link().start(()).unwrap();
    let response = ap.deferred_request("Hello".to_owned());
    assert_eq!(response, "Hello world");
}

/// `AbstractProcess` that times out on a deferred request/response
struct DeferredRequestTimeoutAP;

impl AbstractProcess for DeferredRequestTimeoutAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = ();
    type Handlers = (DeferredRequest<String>,);
    type StartupError = ();

    fn init(_: Config<Self>, _: Self::Arg) -> Result<Self, ()> {
        Ok(Self)
    }
}

impl DeferredRequestHandler<String> for DeferredRequestTimeoutAP {
    type Response = String;

    fn handle(_: State<Self>, _: String, _: DeferredResponse<Self::Response, Self>) {
        // Never return response
    }
}

#[test]
fn deferred_request_timeout() {
    let ap = DeferredRequestTimeoutAP::link().start(()).unwrap();
    let response = ap
        .with_timeout(Duration::from_millis(10))
        .deferred_request("Hello".to_owned());
    assert_eq!(response, Err(Timeout));
}

/// `AbstractProcess` that sends a reference to itself to `parent`, then takes
/// `init_ms` to finish `init`.
struct SlowInitAP;

impl AbstractProcess for SlowInitAP {
    type State = Self;
    type Serializer = Bincode;
    type Arg = (Process<ProcessRef<SlowInitAP>>, Tag, u64);
    type Handlers = ();
    type StartupError = ();

    fn init(config: Config<Self>, (parent, tag, init_ms): Self::Arg) -> Result<Self, ()> {
        parent.tag_send(tag, config.self_ref());
        sleep(Duration::from_millis(init_ms));
        Ok(Self)
    }
}

/// Asserts that nothing else arrives, such as a late `init` result.
fn assert_mailbox_stays_empty<M>(mailbox: Mailbox<M>, wait: Duration)
where
    Bincode: lunatic::serializer::CanSerialize<M>,
{
    assert!(matches!(
        mailbox.receive_timeout(wait),
        Err(MailboxError::TimedOut)
    ));
}

#[test]
fn start_timeout_kills_slow_init(mailbox: Mailbox<ProcessRef<SlowInitAP>>) {
    let tag = Tag::new();
    let config = ProcessConfig::new().unwrap();
    let result = SlowInitAP::configure(&config)
        .start_timeout((mailbox.this(), tag, 500), Duration::from_millis(100));
    assert_eq!(result, Err(StartupError::TimedOut));

    let child = mailbox
        .tag_receive_timeout(&[tag], Duration::from_secs(1))
        .unwrap();
    assert!(!child.is_alive());
    assert_mailbox_stays_empty(mailbox, Duration::from_millis(600));
}

#[test]
fn linked_start_timeout_kills_slow_init(mailbox: Mailbox<ProcessRef<SlowInitAP>>) {
    let tag = Tag::new();
    let result =
        SlowInitAP::link().start_timeout((mailbox.this(), tag, 500), Duration::from_millis(100));
    assert_eq!(result, Err(StartupError::TimedOut));

    // Killing the child must not take this process down through the link.
    let child = mailbox
        .tag_receive_timeout(&[tag], Duration::from_secs(1))
        .unwrap();
    assert!(!child.is_alive());
    assert_mailbox_stays_empty(mailbox, Duration::from_millis(600));
}

#[test]
fn start_timeout_leaves_no_init_result_behind(mailbox: Mailbox<ProcessRef<SlowInitAP>>) {
    let config = ProcessConfig::new().unwrap();
    // `init` takes about as long as the timeout, so it often finishes right
    // after the timeout fired but before the kill landed.
    for attempt in 0..60 {
        let init_ms = 9 + attempt % 3;
        let tag = Tag::new();
        let result = SlowInitAP::configure(&config)
            .start_timeout((mailbox.this(), tag, init_ms), Duration::from_millis(10));
        // A process killed before it ran `init` never reports itself.
        let child = mailbox.tag_receive_timeout(&[tag], Duration::from_millis(100));
        match result {
            Ok(ap) => ap.shutdown(),
            Err(StartupError::TimedOut) => {
                if let Ok(child) = child {
                    assert!(!child.is_alive());
                }
            }
            Err(err) => panic!("unexpected startup error: {err:?}"),
        }
    }
    assert_mailbox_stays_empty(mailbox, Duration::from_millis(100));
}

/// A reference to a process on another node with the caller's own ID.
fn remote_twin() -> ProcessRef<InitOkAP> {
    let this = unsafe { Process::<()>::this() };
    unsafe { ProcessRef::new(this.node_id() + 1, this.id()) }
}

#[test]
#[should_panic(expected = "kill() is not supported for remote processes")]
fn kill_refuses_remote_process() {
    remote_twin().kill();
}

#[test]
#[should_panic(expected = "link_with() is not supported for remote processes")]
fn link_refuses_remote_process() {
    remote_twin().link();
}

#[test]
#[should_panic(expected = "unlink() is not supported for remote processes")]
fn unlink_refuses_remote_process() {
    remote_twin().unlink();
}

#[test]
#[should_panic(expected = "is_alive() is not supported for remote processes")]
fn is_alive_refuses_remote_process() {
    remote_twin().is_alive();
}

#[test]
fn kill() {
    let ap = InitOkAP::start(()).unwrap();
    ap.kill();
    // The kill is delivered asynchronously.
    for _ in 0..1_000 {
        if !ap.is_alive() {
            break;
        }
        sleep(Duration::from_millis(1));
    }
    assert!(!ap.is_alive());
}
