// ABOUTME: Multi-client synchronized playback group
// ABOUTME: Every member receives identical audio bytes with identical timestamps, so each client's clock-sync offset alone yields sample-accurate multi-room sync

use crate::error::Error;
use crate::protocol::messages::{PlayerCommand, StreamPlayerConfig};
use crate::server::binary::encode_audio_frame;
use crate::server::connection::{AudioEnqueue, QueuedControl, ServerSender};
use crate::server::timeline::SharedTimeline;
use crate::sync::raw_clock::Clock;
use futures_util::future::join_all;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio_tungstenite::tungstenite::Bytes;

// Re-exported here for source compatibility; the constant now lives with the
// timeline it parameterizes.
pub use crate::server::timeline::DEFAULT_SEND_AHEAD_US;

/// A synchronized playback group.
///
/// The server-side synchronization trick is simple and doesn't require
/// knowing any client's individual clock: every member is sent the *same*
/// audio bytes tagged with the *same* `server/time`-domain timestamp. Each
/// client independently converts that timestamp into its own clock domain
/// (via the offset/drift it tracks from `client/time`/`server/time`
/// exchanges) and schedules local playback there — so two members with
/// converged clock-sync play the same chunk at the same wall-clock instant
/// without the server ever comparing their clocks to each other.
///
/// The timestamp stream lives in a [`SharedTimeline`]. A group created with
/// [`Group::new`] owns its own timeline (the classic single-group case: sync is
/// automatic). Several groups created with [`Group::with_timeline`] over one
/// `Arc<SharedTimeline>` share a single timeline — the building block for
/// per-device senders that must stay phase-locked while being addressed
/// independently (duck/overlay/route one member without the others). In that
/// mode the caller stamps the timeline **once** per chunk
/// ([`SharedTimeline::stamp`]) and delivers the result to each group via
/// [`Group::push_encoded`], instead of calling [`Group::push_audio`] per group
/// (which would advance the shared timeline once per group). The timeline-owning
/// lifecycle calls ([`Group::start_stream`], [`Group::end_stream`]) refuse to run
/// on a shared timeline, because re-anchoring or clearing it would desync every
/// *other* group sharing it — use [`Group::broadcast_stream_start`] /
/// [`Group::broadcast_stream_end`] plus one explicit
/// [`SharedTimeline::set_config`] instead.
///
/// v1 scope: one shared PCM format for the whole group — no per-client
/// transcoding, so a member that can't take the group's format is a v1
/// limitation, not silently-wrong audio. No late-join catch-up (a client
/// added mid-stream just gets `stream/start` and audio from that point
/// forward) and no historical buffer replay.
pub struct Group {
    timeline: Arc<SharedTimeline>,
    /// Whether this group may re-anchor/clear `timeline` (true unless the
    /// timeline came in via [`Group::with_timeline`], where other groups depend
    /// on it).
    owns_timeline: bool,
    /// The group's members — and, deliberately, its **ordering point**.
    ///
    /// Every frame this group queues for a member, control or audio, is queued
    /// while this lock is held: `stream/start`/`stream/end` in the lifecycle
    /// calls, audio in [`Group::push_encoded`]. Queueing is synchronous
    /// (`ServerSender::queue_*` / `enqueue_audio` never await), so holding one
    /// lock across it is enough to give every member the same, valid order — and
    /// no `await` ever happens while it's held, which a `std::sync::Mutex` makes
    /// hard to get wrong by accident.
    ///
    /// Lock order is `members` before the timeline's own lock, never the
    /// reverse.
    members: Mutex<HashMap<String, ServerSender>>,
}

impl Group {
    /// Create an empty group that owns a fresh timeline in `clock`'s domain —
    /// pass the same clock the [`crate::server::ServerListener`] that accepted
    /// these connections was built with, so timestamps here are in the same
    /// domain as the `server/time` replies members already trust.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            timeline: Arc::new(SharedTimeline::new(clock)),
            owns_timeline: true,
            members: Mutex::new(HashMap::new()),
        }
    }

    /// Create an empty group that shares an existing [`SharedTimeline`] with
    /// other groups/senders. All groups sharing one timeline emit identical
    /// timestamps for the same chunk — see the type docs for the stamp-once
    /// contract, and for why the lifecycle calls behave differently here.
    pub fn with_timeline(timeline: Arc<SharedTimeline>) -> Self {
        Self {
            timeline,
            owns_timeline: false,
            members: Mutex::new(HashMap::new()),
        }
    }

    /// Override the default send-ahead lead time. Only valid on a group that
    /// owns its (as-yet-unshared) timeline, i.e. straight after [`Group::new`];
    /// it rebuilds the timeline with the new lead. A group built by
    /// [`Group::with_timeline`] is left untouched — set the lead on the shared
    /// timeline itself instead.
    pub fn with_send_ahead_us(self, send_ahead_us: i64) -> Self {
        if !self.owns_timeline {
            log::error!(
                "Group::with_send_ahead_us ignored: this group shares its timeline with others"
            );
            return self;
        }
        let clock = self.timeline.clock();
        Self {
            timeline: Arc::new(SharedTimeline::new(clock).with_send_ahead_us(send_ahead_us)),
            owns_timeline: true,
            members: self.members,
        }
    }

    /// The timeline backing this group, so a caller can share it across
    /// per-device senders (`Group::with_timeline(group.timeline())`) and stamp
    /// it once per chunk.
    pub fn timeline(&self) -> Arc<SharedTimeline> {
        Arc::clone(&self.timeline)
    }

    /// Client IDs of every current member.
    pub fn member_ids(&self) -> Vec<String> {
        self.members.lock().unwrap().keys().cloned().collect()
    }

    /// Number of current members.
    pub fn len(&self) -> usize {
        self.members.lock().unwrap().len()
    }

    /// Whether the group has no members.
    pub fn is_empty(&self) -> bool {
        self.members.lock().unwrap().is_empty()
    }

    /// Add a member. If a stream is already active for this group, starts
    /// it for the new member too (matching the group's already-negotiated
    /// format) — but does not replay any audio already delivered to
    /// existing members (no late-join catch-up in v1, see the type docs).
    ///
    /// The new member's `stream/start` is queued *and* the member inserted under
    /// one lock, so a concurrent [`Self::push_encoded`] can't slip audio in
    /// ahead of it. If the `stream/start` write then fails, the member is
    /// removed again and the error returned.
    pub async fn add_member(
        &self,
        client_id: impl Into<String>,
        sender: ServerSender,
    ) -> Result<(), Error> {
        let client_id = client_id.into();
        let queued = {
            let mut members = self.members.lock().unwrap();
            let queued = self
                .timeline
                .config()
                .map(|cfg| sender.queue_stream_start(cfg));
            members.insert(client_id.clone(), sender);
            queued
        };
        if let Some(queued) = queued {
            if let Err(e) = queued.written().await {
                self.members.lock().unwrap().remove(&client_id);
                return Err(e);
            }
        }
        Ok(())
    }

    /// Remove a member, if present. The caller is responsible for actually
    /// disconnecting it (e.g. via [`crate::server::ServerConnection::disconnect`]) —
    /// this only stops future broadcasts from reaching it.
    pub fn remove_member(&self, client_id: &str) -> Option<ServerSender> {
        self.members.lock().unwrap().remove(client_id)
    }

    /// Start (or restart, e.g. after a format change) the shared stream for
    /// every current member, and re-anchor the audio timeline.
    ///
    /// `stream/start` is queued for every member while the member lock is held,
    /// so audio pushed concurrently is ordered either wholly before it (and then
    /// discarded as belonging to the previous stream) or wholly after it. The
    /// client never observes audio *between* the re-anchor and the
    /// `stream/start`.
    ///
    /// Errors if this group shares its timeline with others: re-anchoring would
    /// desync them. Call [`SharedTimeline::set_config`] once and
    /// [`Self::broadcast_stream_start`] per group instead.
    pub async fn start_stream(&self, config: StreamPlayerConfig) -> Result<(), Error> {
        self.require_owned_timeline("start_stream", "broadcast_stream_start")?;
        let queued = {
            let members = self.members.lock().unwrap();
            self.timeline.set_config(config.clone());
            Self::queue_each(&members, |sender| sender.queue_stream_start(config.clone()))
        };
        self.settle(queued).await;
        Ok(())
    }

    /// End the shared stream for every current member and reset the timeline.
    ///
    /// Ordered against concurrent pushes exactly as [`Self::start_stream`] is,
    /// but the other way round: audio pushed before `stream/end` was queued goes
    /// out *first*, since ending a stream means "after everything I sent" —
    /// overtaking it would truncate the tail. Audio pushed concurrently *after*
    /// lands after the `stream/end`, which is the caller's own race, not this
    /// method's.
    ///
    /// Errors if this group shares its timeline with others: clearing it would
    /// strand them without a config. Use [`Self::broadcast_stream_end`] (plus
    /// [`SharedTimeline::clear_config`] when the *last* group is done).
    pub async fn end_stream(&self) -> Result<(), Error> {
        self.require_owned_timeline("end_stream", "broadcast_stream_end")?;
        let queued = {
            let members = self.members.lock().unwrap();
            self.timeline.clear_config();
            Self::queue_each(&members, |sender| sender.queue_stream_end())
        };
        self.settle(queued).await;
        Ok(())
    }

    /// Send `stream/start` to every current member **without** touching the
    /// timeline — the shared-timeline counterpart to [`Self::start_stream`].
    ///
    /// Use when several groups share one [`SharedTimeline`]: set the config once
    /// on the timeline ([`SharedTimeline::set_config`]), then start each group.
    /// Safe on an owned timeline too, where it simply means "re-announce the
    /// stream without re-anchoring".
    pub async fn broadcast_stream_start(&self, config: StreamPlayerConfig) {
        let queued = {
            let members = self.members.lock().unwrap();
            Self::queue_each(&members, |sender| sender.queue_stream_start(config.clone()))
        };
        self.settle(queued).await;
    }

    /// Send `stream/end` to every current member **without** touching the
    /// timeline — the shared-timeline counterpart to [`Self::end_stream`].
    pub async fn broadcast_stream_end(&self) {
        let queued = {
            let members = self.members.lock().unwrap();
            Self::queue_each(&members, |sender| sender.queue_stream_end())
        };
        self.settle(queued).await;
    }

    /// Ask every member to discard buffered-but-unplayed audio (e.g. after a
    /// seek) without ending the stream, and reset the timeline anchor to match.
    /// Audio still queued for a member is dropped rather than written after the
    /// `stream/clear`.
    pub async fn clear_stream(&self) {
        let queued = {
            let members = self.members.lock().unwrap();
            if self.owns_timeline {
                self.timeline.reset();
            }
            Self::queue_each(&members, |sender| sender.queue_stream_clear())
        };
        self.settle(queued).await;
    }

    /// Push one PCM chunk to every member: stamp the timeline once, then fan
    /// the identical frame out. Returns that timestamp. Use this for a group
    /// that owns its timeline. When several groups share one timeline, stamp it
    /// yourself once per chunk and call [`Group::push_encoded`] per group so the
    /// timeline advances only once. Enqueue is non-blocking, so one slow member
    /// never delays the others; a member whose connection has died is pruned.
    pub fn push_audio(&self, pcm: &[u8]) -> i64 {
        // Stamp under the member lock so this chunk's timestamp and its enqueue
        // are one atomic step relative to a concurrent lifecycle transition —
        // otherwise a chunk could be stamped against the old timeline and
        // enqueued after a `stream/end`.
        let mut members = self.members.lock().unwrap();
        let ts = self.timeline.stamp(pcm.len());
        Self::fan_out(&mut members, ts, pcm);
        ts
    }

    /// Fan one PCM chunk out to every member at a caller-supplied timestamp,
    /// **without** advancing the timeline. This is the shared-timeline path:
    /// the caller stamps a shared [`SharedTimeline`] once and delivers that one
    /// `ts` to each group/sender, so every member's chunk-N carries an
    /// identical timestamp. (For a group that owns its timeline, prefer
    /// [`Group::push_audio`], which stamps and fans in one call.)
    pub fn push_encoded(&self, ts: i64, pcm: &[u8]) {
        let mut members = self.members.lock().unwrap();
        Self::fan_out(&mut members, ts, pcm);
    }

    /// Encode once, fan the same frame out to every member as cheap refcount
    /// clones, and prune members whose connection has died.
    fn fan_out(members: &mut HashMap<String, ServerSender>, ts: i64, pcm: &[u8]) {
        let frame: Bytes = encode_audio_frame(ts, pcm).into();
        let mut dead = Vec::new();
        for (id, sender) in members.iter() {
            match sender.enqueue_audio(frame.clone()) {
                Ok(AudioEnqueue::Sent) => {}
                Ok(AudioEnqueue::Evicted) => {
                    log::trace!("group member {id} audio backlog full, dropping chunk")
                }
                Err(_) => dead.push(id.clone()),
            }
        }
        for id in dead {
            log::warn!("dropping dead group member {id}");
            members.remove(&id);
        }
    }

    /// Broadcast a player command (volume, mute, static delay) to every
    /// member. Player commands are independent of the stream, so they overtake
    /// queued audio instead of waiting behind it.
    pub async fn send_player_command(&self, command: PlayerCommand) {
        let queued = {
            let members = self.members.lock().unwrap();
            Self::queue_each(&members, |sender| {
                sender.queue_player_command(command.clone())
            })
        };
        self.settle(queued).await;
    }

    fn require_owned_timeline(&self, method: &str, alternative: &str) -> Result<(), Error> {
        if self.owns_timeline {
            return Ok(());
        }
        Err(Error::Protocol(format!(
            "Group::{method} mutates the timeline, which this group shares with others \
             (every one of them would be desynced); drive the timeline explicitly and use \
             Group::{alternative} per group instead"
        )))
    }

    /// Queue one control frame per member, synchronously, in member order. The
    /// caller holds the member lock across this — that's what makes the
    /// resulting order authoritative.
    fn queue_each(
        members: &HashMap<String, ServerSender>,
        queue: impl Fn(&ServerSender) -> QueuedControl,
    ) -> Vec<(String, QueuedControl)> {
        members
            .iter()
            .map(|(id, sender)| (id.clone(), queue(sender)))
            .collect()
    }

    /// Await already-queued control frames concurrently — a slow member never
    /// delays learning about the others — then drop any member whose write
    /// failed (its writer task is gone or its socket stalled past the write
    /// timeout; either way the failure is permanent).
    async fn settle(&self, queued: Vec<(String, QueuedControl)>) {
        let results = join_all(
            queued
                .into_iter()
                .map(|(id, queued)| async move { (id, queued.written().await) }),
        )
        .await;
        let mut members = self.members.lock().unwrap();
        for (id, result) in results {
            if let Err(e) = result {
                log::warn!("dropping group member {id}: {e}");
                members.remove(&id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::raw_clock::DefaultClock;

    fn pcm_config() -> StreamPlayerConfig {
        StreamPlayerConfig {
            codec: "pcm".to_string(),
            sample_rate: 48000,
            channels: 2,
            bit_depth: 16,
            codec_header: None,
        }
    }

    #[test]
    fn new_group_is_empty() {
        let group = Group::new(Arc::new(DefaultClock::default()));
        assert!(group.is_empty());
        assert_eq!(group.len(), 0);
        assert_eq!(group.member_ids().len(), 0);
    }

    #[test]
    fn groups_can_share_one_timeline() {
        let a = Group::new(Arc::new(DefaultClock::default()));
        let b = Group::with_timeline(a.timeline());
        assert!(Arc::ptr_eq(&a.timeline(), &b.timeline()));
    }

    /// A shared timeline belongs to whoever coordinates the senders, not to any
    /// one group: re-anchoring or clearing it from a single group would desync
    /// every other group sharing it, so those calls are refused outright rather
    /// than silently doing damage.
    #[tokio::test]
    async fn lifecycle_calls_that_mutate_a_shared_timeline_are_refused() {
        let owner = Group::new(Arc::new(DefaultClock::default()));
        let sharer = Group::with_timeline(owner.timeline());

        assert!(sharer.start_stream(pcm_config()).await.is_err());
        assert!(sharer.end_stream().await.is_err());
        // The shared timeline is untouched by the refused calls...
        assert!(owner.timeline().config().is_none());
        // ...while the owner may still drive it.
        assert!(owner.start_stream(pcm_config()).await.is_ok());
        assert!(owner.timeline().config().is_some());
        // And the sharer can still start its own members without touching it.
        sharer.broadcast_stream_start(pcm_config()).await;
        assert!(owner.timeline().config().is_some());
        assert!(owner.end_stream().await.is_ok());
        assert!(owner.timeline().config().is_none());
    }
}
