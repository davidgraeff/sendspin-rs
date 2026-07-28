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
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
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

/// Minimum spacing between `server/time` replies. The spec's cadence is about
/// one `client/time` per second; a peer that asks far faster gains nothing (each
/// reply supersedes the last) and would otherwise convert its own send rate into
/// server work. Requests arriving inside this window are answered by the reply
/// already pending rather than queueing another.
const MIN_TIME_REPLY_INTERVAL_US: i64 = 50_000;

/// Default deadline for the inbound handshake — `client/hello` must arrive, and
/// `server/hello` must be written, within this.
///
/// A peer that completes the WebSocket handshake and then goes silent (or stops
/// reading) would otherwise park the task driving it forever. That matters more
/// than it sounds: [`crate::server::ServerListener::accept`] drives the handshake
/// inline, so one such peer blocks every subsequent inbound connection, and on the
/// dial side it parks a [`crate::server::ClientManager`] supervisor with no
/// backoff progression and no way to redirect it.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// it would truncate the tail of the stream. The flush shares a single
    /// write-timeout budget with the frame it precedes — per-frame deadlines
    /// would let a merely-slow member hold `stream/end` for backlog × timeout —
    /// so once that budget is spent the remaining tail is dropped.
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
    Close {
        ack: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
}

/// A pending `server/time` echo.
///
/// This travels in a single-slot [`watch`] channel rather than a queue, and that
/// is a correctness property, not an optimisation: the reply is derived purely
/// from the *latest* request, so a peer that floods `client/time` can only ever
/// have one outstanding. Queueing one per request instead lets a peer's send rate
/// dictate the server's memory use and, because control frames are written ahead
/// of audio, starve the audio lane to a standstill.
///
/// `server_transmitted` is stamped by the writer immediately before the frame
/// reaches the wire, not here — waiting time would otherwise leak into the
/// client's clock filter as measurement error.
#[derive(Debug, Clone, Copy)]
struct TimeRequest {
    client_transmitted: i64,
    server_received: i64,
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
/// Bounded by `deadline` **overall**, not per frame. Per-frame deadlines would
/// make the flush cost backlog × write_timeout — a member slow enough to keep
/// succeeding could hold `stream/end` (and the close behind it) for minutes. Past
/// the deadline the remaining tail is dropped, which is the right trade: the tail
/// of an ending stream is worth less than the connection.
///
/// Returns the first write error, leaving the rest un-flushed: the connection is
/// finished at that point, so there's nothing to salvage.
async fn flush_audio_before<S>(
    sink: &mut SplitSink<WebSocketStream<S>, WsMessage>,
    audio_rx: &mut UnboundedReceiver<AudioCommand>,
    audio_queued: &AtomicUsize,
    before: u64,
    drop_audio_before: u64,
    deadline: Instant,
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
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = if audio.seq < drop_audio_before || remaining.is_zero() {
            // Superseded by an earlier transition that already went out, or the
            // flush budget is spent and the tail is being dropped.
            Ok(())
        } else {
            write_frame(sink, WsMessage::Binary(audio.frame), remaining).await
        };
        audio_queued.fetch_sub(1, Ordering::Relaxed);
        match result {
            Ok(()) => {
                if let Some(ack) = audio.ack {
                    let _ = ack.send(Ok(()));
                }
            }
            Err(e) => {
                // Report the flush failure as its own thing: the caller is awaiting
                // a lifecycle frame and would otherwise be told its own write
                // stalled, with the error text wrapped twice.
                let propagated = Error::WebSocket(format!(
                    "audio flush before a stream transition failed: {e}"
                ));
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
    mut time_rx: watch::Receiver<Option<TimeRequest>>,
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
        // can never delay one. The time lane sits between the two: it is
        // single-slot, so it can hold at most one frame's worth of priority over
        // audio no matter how fast a peer asks.
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
                                // One budget covers the flushed tail *and* the frame
                                // itself, so the whole operation stays inside two
                                // write timeouts however deep the backlog is.
                                let deadline = Instant::now() + write_timeout;
                                match flush_audio_before(
                                    &mut sink,
                                    &mut audio_rx,
                                    &audio_queued,
                                    seq,
                                    drop_audio_before,
                                    deadline,
                                )
                                .await
                                {
                                    Ok(()) => write_frame(&mut sink, msg, write_timeout).await,
                                    Err(e) => Err(e),
                                }
                            }
                        };
                        let failed = result.is_err();
                        log::debug!("Wrote control frame: ok={}", !failed);
                        // Ignore SendError: the caller may have dropped its receiver.
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
            Ok(()) = time_rx.changed() => {
                // Stamp `server_transmitted` here, immediately before the write, so
                // however long this reply waited behind other frames does not leak
                // into the client's clock filter as measurement error.
                let Some(req) = *time_rx.borrow_and_update() else {
                    continue;
                };
                let reply = Message::ServerTime(ServerTime {
                    client_transmitted: req.client_transmitted,
                    server_received: req.server_received,
                    server_transmitted: clock.now_micros(),
                });
                let result = match serde_json::to_string(&reply) {
                    Ok(json) => {
                        write_frame(&mut sink, WsMessage::Text(json.into()), write_timeout).await
                    }
                    Err(e) => Err(Error::Protocol(e.to_string())),
                };
                if result.is_err() {
                    break;
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
    /// Bounded by the connection's write timeout (see [`DEFAULT_WRITE_TIMEOUT`])
    /// plus whatever control frames were already queued ahead of it. A
    /// `stream/end` additionally waits for the audio pushed before it, but that
    /// flush shares one write-timeout budget with the frame itself, so the total
    /// stays within two write timeouts regardless of backlog depth.
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
        // Liveness is checked before the backlog, and must stay that way. The
        // counter is only decremented by the writer, so frames still queued when
        // the writer exits are never accounted for — leaving the counter at or
        // above the cap on a connection that is already dead. Checking the backlog
        // first would then report `Evicted` forever and the caller would never
        // learn to prune the member.
        if self.audio_tx.is_closed() {
            return Err(Error::WebSocket("connection closed".to_string()));
        }
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

    /// Claim the next audio sequence number.
    ///
    /// Note what this does *not* buy: no memory ordering on this counter can order
    /// the claim against the separate `audio_tx.send` that follows it, so a frame
    /// can be numbered before a control frame computes its marker and still reach
    /// the channel after. What makes the marker exact is the caller holding one
    /// lock across claim *and* send — which is precisely what
    /// [`crate::server::Group`] does. Concurrent pushers on one `ServerSender`
    /// without that lock get best-effort ordering.
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
        // Deliberately no logging here: `Group` calls this while holding the lock
        // that orders frames, and formatting a log record — let alone a subscriber
        // blocking on a full pipe — would extend that critical section without
        // bound. The writer logs what actually went out instead.
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
    /// this connection's pending audio. It can still wait on one in-flight write
    /// plus its own — and, if a `stream/end` is queued ahead of it, on that
    /// frame's flush budget — each bounded by the connection's write timeout, so
    /// this returns even against a socket that has stopped draining entirely.
    ///
    /// Audio still queued when the close is processed is **discarded**: nothing
    /// behind a close is written. Call [`ServerSender::send_stream_end`] first if
    /// the tail of the stream matters.
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
        handshake_timeout: Duration,
    ) -> Result<Self, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut write, mut read) = ws_stream.split();

        // Everything up to the writer task's existence is bounded here, because
        // until it exists there is nothing else to bound it: a peer that finishes
        // the WebSocket handshake and then stays silent, or stops reading, would
        // otherwise park this task forever — and with it the accept loop or the
        // dial supervisor that is driving it.
        let handshake = tokio::time::timeout(
            handshake_timeout,
            Self::handshake(
                &mut write,
                &mut read,
                server_id,
                server_name,
                connection_reason,
                write_timeout,
            ),
        );
        let (hello, active_roles) = match handshake.await {
            Ok(result) => result?,
            Err(_) => {
                return Err(Error::Connection(format!(
                    "handshake did not complete within {handshake_timeout:?}"
                )))
            }
        };

        let (ctrl_tx, ctrl_rx) = unbounded_channel::<ControlCommand>();
        let (audio_tx, audio_rx) = unbounded_channel::<AudioCommand>();
        let (time_tx, time_rx) = watch::channel::<Option<TimeRequest>>(None);
        let (message_tx, message_rx) = unbounded_channel();

        let audio_queued = Arc::new(AtomicUsize::new(0));
        let writer_handle = tokio::spawn(writer_task(
            write,
            ctrl_rx,
            time_rx,
            audio_rx,
            Arc::clone(&clock),
            Arc::clone(&audio_queued),
            write_timeout,
        ));

        let router_handle = tokio::spawn(async move {
            Self::message_router(read, message_tx, time_tx, clock).await;
        });

        let sender = ServerSender {
            ctrl_tx,
            audio_tx,
            audio_queued,
            audio_seq: Arc::new(AtomicU64::new(0)),
        };
        Ok(Self {
            hello,
            active_roles: active_roles.clone(),
            messages: message_rx,
            sender: sender.clone(),
            guard: ServerConnectionGuard {
                sender,
                router_handle: Some(router_handle),
                writer_handle: Some(writer_handle),
            },
        })
    }

    /// Read `client/hello`, negotiate roles, reply `server/hello`. Split out of
    /// [`Self::drive`] so the whole exchange can sit inside one deadline.
    async fn handshake<S>(
        write: &mut SplitSink<WebSocketStream<S>, WsMessage>,
        read: &mut SplitStream<WebSocketStream<S>>,
        server_id: &str,
        server_name: &str,
        connection_reason: ConnectionReason,
        write_timeout: Duration,
    ) -> Result<(ClientHello, Vec<String>), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
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
        // Bounded like every other write: this one runs before the writer task
        // exists, so it needs its own deadline rather than inheriting one.
        write_frame(write, WsMessage::Text(json.into()), write_timeout).await?;

        Ok((hello, active_roles))
    }

    async fn message_router<S>(
        mut read: SplitStream<WebSocketStream<S>>,
        message_tx: UnboundedSender<Message>,
        time_tx: watch::Sender<Option<TimeRequest>>,
        clock: Arc<dyn Clock>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut message_closed = false;
        let mut last_time_reply_us: Option<i64> = None;

        while let Some(msg) = read.next().await {
            match msg {
                Ok(WsMessage::Text(text)) => {
                    // Capture receive time before deserialization so
                    // `server_received` is as close to true arrival as possible.
                    let server_received = clock.now_micros();
                    match serde_json::from_str::<Message>(&text) {
                        Ok(Message::ClientTime(t)) => {
                            // Rate-limit, then coalesce. A peer asking faster than
                            // the spec's ~1/s cadence gains nothing — each reply
                            // supersedes the last — but answering every request
                            // would let its send rate drive our work and memory.
                            let too_soon = last_time_reply_us.is_some_and(|last| {
                                server_received - last < MIN_TIME_REPLY_INTERVAL_US
                            });
                            if too_soon {
                                log::trace!("Ignoring client/time inside the reply interval");
                                continue;
                            }
                            last_time_reply_us = Some(server_received);
                            if time_tx
                                .send(Some(TimeRequest {
                                    client_transmitted: t.client_transmitted,
                                    server_received,
                                }))
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
