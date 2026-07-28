// ABOUTME: Server-role implementation of the Sendspin protocol
// ABOUTME: Accepts or dials player clients, syncs clocks, streams audio to synchronized multi-client groups

// Handles both connection directions: clients that dial in
// (`ServerListener::accept`) and clients that only run their own embedded
// server and must be discovered over mDNS and dialed (`ClientBrowser` +
// `dial_client`, or the supervised `ClientManager`).
//
// Each connection has one writer task fed by two queues: a control lane (stream
// lifecycle, player commands, close) and a data lane (audio). Control is
// dequeued ahead of audio, so a member with a backlog of audio — or a socket
// that has stopped draining entirely — can't delay a volume change or its own
// disconnect. Every write is bounded by a timeout (`DEFAULT_WRITE_TIMEOUT`), so
// a stalled socket becomes a dead connection rather than a stuck writer.
//
// Because control overtakes audio, each control frame declares how it relates to
// the audio it just overtook: a player command ignores it, `stream/start` and
// `stream/clear` discard it (it belongs to a stream they supersede), and
// `stream/end` writes it out first (it means "after everything I sent", so
// overtaking would truncate the tail). See `connection.rs` for the mechanism and
// `Group` for how the two lanes are kept in a single, valid order.
//
// Not yet supported: per-client codec transcoding (one PCM format per group),
// the non-player roles (color, visualizer, artwork, controller, metadata),
// external player registration, and late-join history replay — a client that
// joins mid-stream receives the current stream and all subsequent audio,
// synchronized with existing members, but nothing buffered from before it
// joined.

mod binary;
mod connection;
mod dial;
mod discovery;
mod group;
mod listener;
mod manager;
mod timeline;

pub use binary::encode_audio_frame;
pub use connection::{
    AudioEnqueue, QueuedControl, ServerConnection, ServerConnectionGuard, ServerSender,
    DEFAULT_WRITE_TIMEOUT,
};
pub use dial::{dial_client, dial_client_with_write_timeout};
pub use discovery::{Advertisement, ClientBrowser, Discovered};
pub use group::{Group, DEFAULT_SEND_AHEAD_US};
pub use listener::ServerListener;
pub use manager::{ClientEvent, ClientManager};
pub use timeline::SharedTimeline;
