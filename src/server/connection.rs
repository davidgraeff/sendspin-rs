// ABOUTME: Per-client connection actor for the server role: drives the
// ABOUTME: server-side handshake, time-sync echo, and message dispatch.

use crate::error::Error;
use crate::protocol::messages::{
    ClientHello, ConnectionReason, Message, PlayerCommand, ServerCommand, ServerHello, ServerTime,
    StreamClear, StreamEnd, StreamPlayerConfig, StreamStart,
};
use crate::server::binary::encode_audio_frame;
use crate::sync::raw_clock::Clock;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::{
    tungstenite::{Bytes, Message as WsMessage},
    WebSocketStream,
};

/// The only role this server negotiates in v1. See the crate-level server
/// docs for the list of roles deferred for a later contribution
/// (color/visualizer/artwork/controller/metadata).
const PLAYER_ROLE: &str = "player@v1";

/// Maximum audio frames a single connection may have queued but not yet
/// written before [`ServerSender::enqueue_audio`] starts dropping frames. This
/// bounds memory for a slow or stalled member so it can't back up the whole
/// process — its own audio suffers, nobody else's does.
const MAX_QUEUED_AUDIO_FRAMES: usize = 32;

/// Default deadline for a single WebSocket write before the connection is
/// declared dead (override with [`crate::server::ServerListener::write_timeout`]).
///
/// A member whose socket stops draining — a client that dropped off the WiFi
/// while holding the TCP connection open, so the kernel send buffer fills and
/// never empties — must not park the writer task forever. The writer is the
/// only thing that touches the socket, so a write that never completes stalls
/// *everything* behind it: `stream/end`, a volume command, the close handshake.
/// Every caller awaiting one of those would wait with it. Bounding each write
/// converts that indefinite hang into a dead connection: the writer exits,
/// every [`ServerSender`] method starts returning `Err`, and
/// [`crate::server::Group`] prunes the member. A real client re-dials.
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Outcome of a non-blocking [`ServerSender::enqueue_audio`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioEnqueue {
    /// The frame was queued for the writer task.
    Sent,
    /// The connection's audio backlog was at capacity; the frame was dropped.
    Evicted,
}

/// What a control frame does about the audio already queued ahead of it.
///
/// Control frames are dequeued ahead of audio (that's the whole point of the
/// separate lane), so each one has to say how it relates to the audio it just
/// overtook. Getting this wrong is how a client ends up seeing `stream/end`
/// followed by audio — or, in the other direction, how the tail of a stream gets
/// truncated.
///
/// The `u64` in the ordered variants is the audio sequence number this frame was
/// queued at: audio below it was pushed earlier, audio at or above it was pushed
/// later and is always left alone.
#[derive(Debug, Clone, Copy)]
enum AudioOrdering {
    /// Unrelated to the audio stream — write it now and leave the queue alone.
    /// Player commands (volume, mute, static delay) take effect on arrival, so
    /// overtaking audio is exactly what's wanted.
    Independent,
    /// Write it now and *discard* the audio queued before it. That audio belongs
    /// to a stream this frame supersedes (`stream/start` after a format change)
    /// or explicitly invalidates (`stream/clear`), so delivering it afterwards
    /// would be wrong, not merely late.
    Supersede(u64),
    /// Write the audio queued before it *first*, then the frame itself.
    /// `stream/end` logically follows everything already pushed, so overtaking
    /// it would truncate the tail of the stream. The flush is still bounded: the
    /// first write that stalls past the write timeout fails the whole thing, so
    /// a dead member can't hold `stream/end` for backlog × timeout.
    Flush(u64),
}

/// A control-plane command for the writer task.
///
/// Control travels on its own channel and is dequeued ahead of queued audio (see
/// [`writer_task`]), so a backlog of audio for a slow member can't delay a
/// player command or the close handshake — and, together with [`AudioOrdering`],
/// can't reorder a stream lifecycle transition against the audio around it
/// either.
enum ControlCommand {
    Send {
        msg: WsMessage,
        ordering: AudioOrdering,
        ack: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
    /// `server/time` reply: `server_transmitted` is stamped from the clock
    /// immediately before the frame reaches the wire, not when this command
    /// was enqueued — queueing delay would otherwise leak into the client's
    /// clock filter as measurement error (this is why it's its own variant
    /// rather than a pre-built `Send`).
    TimeReply {
        client_transmitted: i64,
        server_received: i64,
        ack: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
    Close {
        ack: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
}

/// One data-plane (audio) frame for the writer task.
struct AudioCommand {
    /// Enqueue order within this connection, compared against a control frame's
    /// `purge_audio_before`.
    seq: u64,
    frame: Bytes,
    /// `Some` only for [`ServerSender::send_audio_chunk`], which awaits its own
    /// frame. The group broadcast path ([`ServerSender::enqueue_audio`]) is
    /// fire-and-forget, so broadcasting never blocks on any member's socket.
    ack: Option<tokio::sync::oneshot::Sender<Result<(), Error>>>,
}

/// Write one frame, bounded by `write_timeout`.
///
/// `SinkExt::send` is not cancel-safe, so timing out can leave the sink
/// mid-frame — that's acceptable only because the caller treats a timeout as
/// fatal: the writer loop exits and drops the sink, and nothing else ever
/// touches it.
async fn write_frame<S>(
    sink: &mut SplitSink<WebSocketStream<S>, WsMessage>,
    msg: WsMessage,
    write_timeout: Duration,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match tokio::time::timeout(write_timeout, sink.send(msg)).await {
        Ok(result) => result.map_err(|e| Error::WebSocket(e.to_string())),
        Err(_) => Err(Error::WebSocket(format!(
            "write stalled for {write_timeout:?}"
        ))),
    }
}

/// Write out every already-queued audio frame with `seq < before`, so a control
/// frame that logically *follows* that audio ([`AudioOrdering::Flush`]) doesn't
/// overtake it.
///
/// Only frames already sitting in `audio_rx` are flushed, which is exactly the
/// right set: the caller queued this control frame after those audio frames, and
/// an unbounded channel's `send` completes immediately, so anything below `before`
/// is already here. (A caller enqueueing audio concurrently from another task
/// without serializing against the control frame gets best-effort ordering — see
/// [`QueuedControl`] for how [`crate::server::Group`] serializes the two.)
///
/// Returns the first write error, leaving the rest un-flushed: the connection is
/// finished at that point, so there's nothing to salvage.
async fn flush_audio_before<S>(
    sink: &mut SplitSink<WebSocketStream<S>, WsMessage>,
    audio_rx: &mut UnboundedReceiver<AudioCommand>,
    audio_queued: &AtomicUsize,
    before: u64,
    drop_audio_before: u64,
    write_timeout: Duration,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    while let Ok(audio) = audio_rx.try_recv() {
        // A frame at or above the marker was pushed after this control frame was
        // queued, so it isn't ours to flush — but it's already out of the channel
        // and re-queueing it would put it behind whatever arrived since, so write
        // it and stop. That also bounds the flush against a task that keeps
        // pushing audio concurrently.
        let reached_marker = audio.seq >= before;
        let result = if audio.seq < drop_audio_before {
            // Superseded by an earlier transition that already went out.
            Ok(())
        } else {
            write_frame(sink, WsMessage::Binary(audio.frame), write_timeout).await
        };
        audio_queued.fetch_sub(1, Ordering::Relaxed);
        match result {
            Ok(()) => {
                if let Some(ack) = audio.ack {
                    let _ = ack.send(Ok(()));
                }
            }
            Err(e) => {
                let propagated = Error::WebSocket(e.to_string());
                if let Some(ack) = audio.ack {
                    let _ = ack.send(Err(e));
                }
                return Err(propagated);
            }
        }
        if reached_marker {
            break;
        }
    }
    Ok(())
}

async fn writer_task<S>(
    mut sink: SplitSink<WebSocketStream<S>, WsMessage>,
    mut ctrl_rx: UnboundedReceiver<ControlCommand>,
    mut audio_rx: UnboundedReceiver<AudioCommand>,
    clock: Arc<dyn Clock>,
    audio_queued: Arc<AtomicUsize>,
    write_timeout: Duration,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Audio whose sequence number is below this was enqueued before a lifecycle
    // transition that has already been written, so writing it now would put it
    // on the wrong side of that transition. See `purge_audio_before`.
    let mut drop_audio_before: u64 = 0;

    loop {
        // `biased` makes this a strict priority rather than a random choice:
        // whenever a control frame is queued it is taken first, so queued audio
        // can never delay one.
        tokio::select! {
            biased;
            Some(cmd) = ctrl_rx.recv() => {
                match cmd {
                    ControlCommand::Send { msg, ordering, ack } => {
                        let result = match ordering {
                            AudioOrdering::Independent => {
                                write_frame(&mut sink, msg, write_timeout).await
                            }
                            AudioOrdering::Supersede(seq) => {
                                drop_audio_before = drop_audio_before.max(seq);
                                write_frame(&mut sink, msg, write_timeout).await
                            }
                            AudioOrdering::Flush(seq) => {
                                match flush_audio_before(
                                    &mut sink,
                                    &mut audio_rx,
                                    &audio_queued,
                                    seq,
                                    drop_audio_before,
                                    write_timeout,
                                )
                                .await
                                {
                                    Ok(()) => write_frame(&mut sink, msg, write_timeout).await,
                                    Err(e) => Err(e),
                                }
                            }
                        };
                        let failed = result.is_err();
                        // Ignore SendError: the caller may have dropped its receiver.
                        let _ = ack.send(result);
                        if failed {
                            break;
                        }
                    }
                    ControlCommand::TimeReply { client_transmitted, server_received, ack } => {
                        let reply = Message::ServerTime(ServerTime {
                            client_transmitted,
                            server_received,
                            server_transmitted: clock.now_micros(),
                        });
                        let result = match serde_json::to_string(&reply) {
                            Ok(json) => {
                                write_frame(&mut sink, WsMessage::Text(json.into()), write_timeout).await
                            }
                            Err(e) => Err(Error::Protocol(e.to_string())),
                        };
                        let failed = result.is_err();
                        let _ = ack.send(result);
                        if failed {
                            break;
                        }
                    }
                    ControlCommand::Close { ack } => {
                        // No purge watermark needed: the loop exits below, so
                        // nothing queued behind a close is ever written.
                        let result = match tokio::time::timeout(write_timeout, sink.close()).await {
                            Ok(result) => result.map_err(|e| Error::WebSocket(e.to_string())),
                            Err(_) => Err(Error::WebSocket(format!(
                                "close stalled for {write_timeout:?}"
                            ))),
                        };
                        let _ = ack.send(result);
                        break;
                    }
                }
            }
            Some(audio) = audio_rx.recv() => {
                let result = if audio.seq < drop_audio_before {
                    log::trace!("dropping audio frame superseded by a stream lifecycle transition");
                    Ok(())
                } else {
                    write_frame(&mut sink, WsMessage::Binary(audio.frame), write_timeout).await
                };
                audio_queued.fetch_sub(1, Ordering::Relaxed);
                let failed = result.is_err();
                if let Some(ack) = audio.ack {
                    let _ = ack.send(result);
                }
                if failed {
                    break;
                }
            }
            else => break,
        }
    }
    log::debug!("Server connection writer task exiting");
}

/// A control frame that has been *placed in* a connection's write queue, but
/// not yet written.
///
/// Queueing is synchronous. That's the point: a caller holding a lock can fix
/// the order in which the client will observe frames — `stream/start`, audio,
/// `stream/end` — and only then release the lock and await the writes. It's
/// what lets [`crate::server::Group`] serialize lifecycle transitions against
/// concurrent audio pushes without ever awaiting while holding its member lock.
#[must_use = "a queued control frame should be awaited (via `written`) so write failures are noticed"]
pub struct QueuedControl {
    /// `Err` when the frame could not even be queued (serialization failed, or
    /// the writer task is gone), so `written()` can report it uniformly.
    result: Result<tokio::sync::oneshot::Receiver<Result<(), Error>>, Error>,
}

impl QueuedControl {
    /// Wait for this frame to reach the socket.
    ///
    /// Bounded by the connection's write timeout (see
    /// [`DEFAULT_WRITE_TIMEOUT`]) plus whatever control frames were already
    /// queued ahead of it — never by queued audio.
    pub async fn written(self) -> Result<(), Error> {
        match self.result {
            Ok(ack) => ack
                .await
                .map_err(|_| Error::WebSocket("connection closed".to_string()))?,
            Err(e) => Err(e),
        }
    }
}

/// Sender half of a server-role connection. Cheap to clone; all clones share
/// the same underlying connection, audio backlog counter, and frame ordering.
#[derive(Debug, Clone)]
pub struct ServerSender {
    ctrl_tx: UnboundedSender<ControlCommand>,
    audio_tx: UnboundedSender<AudioCommand>,
    audio_queued: Arc<AtomicUsize>,
    audio_seq: Arc<AtomicU64>,
}

impl ServerSender {
    /// Enqueue one pre-encoded audio frame without waiting for it to reach the
    /// wire — a group broadcast calls this on every member, so it must never
    /// block on any one member's socket. `frame` is a [`Bytes`], so fanning the
    /// same frame out to N members is N cheap refcount clones, not N copies.
    ///
    /// Returns [`AudioEnqueue::Evicted`] if this connection's audio backlog is
    /// already full (a slow/stalled member), dropping the frame rather than
    /// growing memory without bound. `Err` means the writer task is gone (the
    /// member is dead) and the caller should stop broadcasting to it.
    pub fn enqueue_audio(&self, frame: Bytes) -> Result<AudioEnqueue, Error> {
        if self.audio_queued.load(Ordering::Relaxed) >= MAX_QUEUED_AUDIO_FRAMES {
            return Ok(AudioEnqueue::Evicted);
        }
        self.audio_queued.fetch_add(1, Ordering::Relaxed);
        let cmd = AudioCommand {
            seq: self.next_audio_seq(),
            frame,
            ack: None,
        };
        match self.audio_tx.send(cmd) {
            Ok(()) => Ok(AudioEnqueue::Sent),
            Err(_) => {
                self.audio_queued.fetch_sub(1, Ordering::Relaxed);
                Err(Error::WebSocket("connection closed".to_string()))
            }
        }
    }

    /// Claim the next audio sequence number. `AcqRel`/`Acquire` (rather than
    /// `Relaxed`) so the ordering between claiming a sequence number here and
    /// reading the counter in [`Self::queue_control`] holds on its own, without
    /// depending on the caller's lock to provide it.
    fn next_audio_seq(&self) -> u64 {
        self.audio_seq.fetch_add(1, Ordering::AcqRel)
    }

    /// Queue one control frame with the given relationship to the audio queued
    /// ahead of it (see [`AudioOrdering`]). `mark` builds that relationship from
    /// the audio sequence number this frame is queued at.
    fn queue_control(
        &self,
        msg: Message,
        mark: impl FnOnce(u64) -> AudioOrdering,
    ) -> QueuedControl {
        let json = match serde_json::to_string(&msg) {
            Ok(json) => json,
            Err(e) => {
                return QueuedControl {
                    result: Err(Error::Protocol(e.to_string())),
                }
            }
        };
        log::debug!("Queueing message: {}", json);
        let ordering = mark(self.audio_seq.load(Ordering::Acquire));
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let cmd = ControlCommand::Send {
            msg: WsMessage::Text(json.into()),
            ordering,
            ack: ack_tx,
        };
        QueuedControl {
            result: match self.ctrl_tx.send(cmd) {
                Ok(()) => Ok(ack_rx),
                Err(_) => Err(Error::WebSocket("connection closed".to_string())),
            },
        }
    }

    /// Queue the start of a player audio stream, fixing its position in this
    /// connection's frame order without awaiting the write. Audio queued before
    /// it belongs to the previous stream and is discarded; audio pushed after it
    /// is unaffected.
    pub fn queue_stream_start(&self, player: StreamPlayerConfig) -> QueuedControl {
        self.queue_control(
            Message::StreamStart(StreamStart {
                player: Some(player),
                artwork: None,
                visualizer: None,
            }),
            AudioOrdering::Supersede,
        )
    }

    /// Queue the end of the player audio stream. Audio pushed before it is
    /// written *first* — `stream/end` means "after everything I sent", so
    /// overtaking it would cut off the tail of the stream.
    pub fn queue_stream_end(&self) -> QueuedControl {
        self.queue_control(
            Message::StreamEnd(StreamEnd {
                roles: Some(vec!["player".to_string()]),
            }),
            AudioOrdering::Flush,
        )
    }

    /// Queue a `stream/clear`. Dropping the queued-but-unwritten audio is
    /// exactly this message's own semantics, applied one hop earlier — there's
    /// no point writing audio the client is being told to discard.
    pub fn queue_stream_clear(&self) -> QueuedControl {
        self.queue_control(
            Message::StreamClear(StreamClear {
                roles: Some(vec!["player".to_string()]),
            }),
            AudioOrdering::Supersede,
        )
    }

    /// Queue a player command (volume, mute, static delay). These are
    /// independent of the audio stream — they take effect as soon as they
    /// arrive — so they overtake queued audio without disturbing any of it.
    pub fn queue_player_command(&self, command: PlayerCommand) -> QueuedControl {
        self.queue_control(
            Message::ServerCommand(ServerCommand {
                player: Some(command),
            }),
            |_| AudioOrdering::Independent,
        )
    }

    /// Announce the start of a player audio stream. Send this once before
    /// the first [`Self::send_audio_chunk`].
    pub async fn send_stream_start(&self, player: StreamPlayerConfig) -> Result<(), Error> {
        self.queue_stream_start(player).written().await
    }

    /// Push one player audio chunk. `timestamp_us` is the intended playback
    /// time in this server's clock domain (see [`crate::sync::raw_clock::Clock`]);
    /// the client converts it to its own domain using the offset/drift it
    /// tracks from `server/time` replies.
    ///
    /// Travels the same data-plane queue as [`Self::enqueue_audio`], so mixing
    /// the two keeps audio in push order; awaiting the write is the only
    /// difference.
    pub async fn send_audio_chunk(&self, timestamp_us: i64, payload: &[u8]) -> Result<(), Error> {
        let frame = encode_audio_frame(timestamp_us, payload);
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.audio_queued.fetch_add(1, Ordering::Relaxed);
        let cmd = AudioCommand {
            seq: self.next_audio_seq(),
            frame: frame.into(),
            ack: Some(ack_tx),
        };
        if self.audio_tx.send(cmd).is_err() {
            self.audio_queued.fetch_sub(1, Ordering::Relaxed);
            return Err(Error::WebSocket("connection closed".to_string()));
        }
        ack_rx
            .await
            .map_err(|_| Error::WebSocket("connection closed".to_string()))?
    }

    /// End the player audio stream.
    pub async fn send_stream_end(&self) -> Result<(), Error> {
        self.queue_stream_end().written().await
    }

    /// Ask the client to discard any buffered-but-unplayed audio (e.g. after
    /// a seek), without ending the stream.
    pub async fn send_stream_clear(&self) -> Result<(), Error> {
        self.queue_stream_clear().written().await
    }

    /// Send a player command (volume, mute, static delay) to the client.
    pub async fn send_player_command(&self, command: PlayerCommand) -> Result<(), Error> {
        self.queue_player_command(command).written().await
    }
}

/// Aborts background tasks on drop. Hold this alive for the lifetime of the
/// connection — mirrors [`crate::protocol::client::ConnectionGuard`].
pub struct ServerConnectionGuard {
    sender: ServerSender,
    router_handle: Option<tokio::task::JoinHandle<()>>,
    writer_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ServerConnectionGuard {
    /// Close the connection. Unlike the client role, the server has no
    /// `goodbye` message of its own to send — it just closes the socket
    /// (optionally after the caller has already sent `stream/end`).
    ///
    /// The close command travels the control lane, so it is never queued behind
    /// this connection's pending audio. It can still wait on one in-flight
    /// write plus its own, both bounded by the connection's write timeout — so
    /// this returns even against a socket that has stopped draining entirely.
    pub async fn disconnect(mut self) -> Result<(), Error> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let close_result = self
            .sender
            .ctrl_tx
            .send(ControlCommand::Close { ack: ack_tx })
            .map_err(|_| Error::WebSocket("connection closed".to_string()));
        let result = match close_result {
            Ok(()) => ack_rx
                .await
                .map_err(|_| Error::WebSocket("connection closed".to_string()))?,
            Err(e) => Err(e),
        };
        if let Some(h) = self.writer_handle.take() {
            let _ = h.await;
        }
        if let Some(h) = self.router_handle.take() {
            h.abort();
        }
        result
    }
}

impl Drop for ServerConnectionGuard {
    fn drop(&mut self) {
        if let Some(h) = self.router_handle.take() {
            h.abort();
        }
        if let Some(h) = self.writer_handle.take() {
            h.abort();
        }
    }
}

/// A single accepted client, past the handshake. Returned by
/// [`crate::server::ServerListener::accept`].
pub struct ServerConnection {
    /// The client's `client/hello` payload — identity, declared capabilities,
    /// device info. Kept in full so callers can read `player@v1_support`
    /// (supported formats, buffer capacity) before starting a stream.
    hello: ClientHello,
    /// Roles this server granted this client (currently always `["player@v1"]`
    /// if the client declared support for it, else empty).
    active_roles: Vec<String>,
    /// `client/state`, `client/command`, and `client/goodbye` messages,
    /// forwarded as received. `client/time` is consumed internally (time-sync
    /// echo) and never forwarded here — same convention as
    /// [`crate::protocol::client::Connection::messages`].
    messages: UnboundedReceiver<Message>,
    sender: ServerSender,
    guard: ServerConnectionGuard,
}

impl ServerConnection {
    /// The client's `client/hello` payload.
    pub fn hello(&self) -> &ClientHello {
        &self.hello
    }

    /// Convenience accessor for `hello().client_id`.
    pub fn client_id(&self) -> &str {
        &self.hello.client_id
    }

    /// Roles granted to this client.
    pub fn active_roles(&self) -> &[String] {
        &self.active_roles
    }

    /// A cheap-to-clone sender for pushing stream control and audio messages
    /// to this client, usable independently of `&mut self`.
    pub fn sender(&self) -> ServerSender {
        self.sender.clone()
    }

    /// Receive the next `client/state`, `client/command`, or `client/goodbye`
    /// message. Returns `None` once the connection has closed.
    pub async fn recv_message(&mut self) -> Option<Message> {
        self.messages.recv().await
    }

    /// Close the connection.
    pub async fn disconnect(self) -> Result<(), Error> {
        self.guard.disconnect().await
    }

    /// Drive the server-side handshake and message loop over an
    /// already-handshaked WebSocket stream. Shared by
    /// [`crate::server::ServerListener::accept`] and tests.
    pub(crate) async fn drive<S>(
        ws_stream: WebSocketStream<S>,
        server_id: &str,
        server_name: &str,
        connection_reason: ConnectionReason,
        clock: Arc<dyn Clock>,
        write_timeout: Duration,
    ) -> Result<Self, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut write, mut read) = ws_stream.split();

        log::debug!("Waiting for client/hello...");
        let hello = loop {
            let Some(result) = read.next().await else {
                return Err(Error::Connection(
                    "connection closed before client/hello".to_string(),
                ));
            };
            match result {
                Ok(WsMessage::Text(text)) => {
                    let msg: Message = serde_json::from_str(&text).map_err(|e| {
                        log::warn!("Failed to parse client message: {} (payload: {})", e, text);
                        Error::Protocol(e.to_string())
                    })?;
                    match msg {
                        Message::ClientHello(hello) => {
                            if hello.version != 1 {
                                return Err(Error::Protocol(format!(
                                    "unsupported protocol version {} (only 1 is supported)",
                                    hello.version
                                )));
                            }
                            break hello;
                        }
                        other => {
                            return Err(Error::Protocol(format!(
                                "expected client/hello, got {:?}",
                                other
                            )))
                        }
                    }
                }
                Ok(WsMessage::Ping(_)) | Ok(WsMessage::Pong(_)) => continue,
                Ok(WsMessage::Close(_)) => {
                    return Err(Error::Connection("client closed connection".to_string()))
                }
                Ok(_) => continue,
                Err(e) => return Err(Error::WebSocket(e.to_string())),
            }
        };
        log::debug!("Received client/hello: {:?}", hello);

        let active_roles: Vec<String> = if hello.supported_roles.iter().any(|r| r == PLAYER_ROLE) {
            vec![PLAYER_ROLE.to_string()]
        } else {
            Vec::new()
        };

        let server_hello = Message::ServerHello(ServerHello {
            server_id: server_id.to_string(),
            name: server_name.to_string(),
            version: 1,
            active_roles: active_roles.clone(),
            connection_reason,
        });
        let json =
            serde_json::to_string(&server_hello).map_err(|e| Error::Protocol(e.to_string()))?;
        write
            .send(WsMessage::Text(json.into()))
            .await
            .map_err(|e| Error::WebSocket(e.to_string()))?;

        let (ctrl_tx, ctrl_rx) = unbounded_channel::<ControlCommand>();
        let (audio_tx, audio_rx) = unbounded_channel::<AudioCommand>();
        let (message_tx, message_rx) = unbounded_channel();

        let audio_queued = Arc::new(AtomicUsize::new(0));
        let writer_handle = tokio::spawn(writer_task(
            write,
            ctrl_rx,
            audio_rx,
            Arc::clone(&clock),
            Arc::clone(&audio_queued),
            write_timeout,
        ));

        let ctrl_tx_router = ctrl_tx.clone();
        let router_handle = tokio::spawn(async move {
            Self::message_router(read, message_tx, ctrl_tx_router, clock).await;
        });

        let sender = ServerSender {
            ctrl_tx,
            audio_tx,
            audio_queued,
            audio_seq: Arc::new(AtomicU64::new(0)),
        };
        Ok(Self {
            hello,
            active_roles,
            messages: message_rx,
            sender: sender.clone(),
            guard: ServerConnectionGuard {
                sender,
                router_handle: Some(router_handle),
                writer_handle: Some(writer_handle),
            },
        })
    }

    async fn message_router<S>(
        mut read: SplitStream<WebSocketStream<S>>,
        message_tx: UnboundedSender<Message>,
        ctrl_tx: UnboundedSender<ControlCommand>,
        clock: Arc<dyn Clock>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut message_closed = false;

        while let Some(msg) = read.next().await {
            match msg {
                Ok(WsMessage::Text(text)) => {
                    // Capture receive time before deserialization so
                    // `server_received` is as close to true arrival as possible.
                    let server_received = clock.now_micros();
                    match serde_json::from_str::<Message>(&text) {
                        Ok(Message::ClientTime(t)) => {
                            let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel();
                            if ctrl_tx
                                .send(ControlCommand::TimeReply {
                                    client_transmitted: t.client_transmitted,
                                    server_received,
                                    ack: ack_tx,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(Message::ClientHello(_)) => {
                            log::warn!("Ignoring unexpected client/hello after handshake");
                        }
                        Ok(msg) => {
                            log::debug!("Received message: {:?}", msg);
                            if !message_closed && message_tx.send(msg).is_err() {
                                log::error!(
                                    "Message receiver dropped — messages will be discarded"
                                );
                                message_closed = true;
                            }
                        }
                        Err(e) => {
                            log::warn!("Failed to parse message: {} (payload: {})", e, text);
                        }
                    }
                }
                Ok(WsMessage::Binary(_)) => {
                    // A client never sends binary frames in the current
                    // protocol (audio/artwork/visualizer are server->client
                    // only); log and ignore rather than erroring the
                    // connection over a forward-compatible future frame.
                    log::warn!("Ignoring unexpected binary frame from client");
                }
                Ok(WsMessage::Ping(_)) | Ok(WsMessage::Pong(_)) => {}
                Ok(WsMessage::Close(_)) => {
                    log::info!("Client closed connection");
                    break;
                }
                Err(e) => {
                    log::warn!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        log::debug!("Message router: WebSocket stream ended");
    }
}
