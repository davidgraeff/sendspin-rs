// ABOUTME: Integration tests for the per-connection writer's two lanes:
// ABOUTME: control frames never wait behind queued audio, a stalled socket can't
// ABOUTME: park control or close forever, and lifecycle/audio order stays valid.

use futures_util::{SinkExt, StreamExt};
use sendspin::protocol::client::AudioChunk;
use sendspin::protocol::messages::{
    ClientHello, Message, PlayerCommand, PlayerCommandType, StreamPlayerConfig,
};
use sendspin::server::{AudioEnqueue, Group, ServerSender};
use sendspin::ServerListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Loop bound for "push until the backlog caps" probes.
const MAX_BACKLOG_PROBE: usize = 200;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

type PeerRead = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

fn test_hello(client_id: &str) -> ClientHello {
    ClientHello {
        client_id: client_id.to_string(),
        name: "Test Player".to_string(),
        version: 1,
        supported_roles: vec!["player@v1".to_string()],
        device_info: None,
        player_v1_support: None,
        artwork_v1_support: None,
        visualizer_v1_support: None,
    }
}

/// Connects a bare peer that plays the client role manually and then reads
/// nothing further until the test asks it to — which is what lets these tests
/// control exactly how much the server's socket can drain.
async fn connect_peer(url: &str, client_id: &str) -> PeerRead {
    let (ws, _) = connect_async(url).await.expect("ws connect");
    let (mut write, mut read) = ws.split();
    let hello = serde_json::to_string(&Message::ClientHello(test_hello(client_id))).unwrap();
    write.send(WsMessage::Text(hello.into())).await.unwrap();
    read.next().await.expect("no server/hello").unwrap(); // discard server/hello
    read
}

fn pcm_config() -> StreamPlayerConfig {
    StreamPlayerConfig {
        codec: "pcm".to_string(),
        sample_rate: 48000,
        channels: 2,
        bit_depth: 16,
        codec_header: None,
    }
}

fn volume(v: u8) -> PlayerCommand {
    PlayerCommand {
        command: PlayerCommandType::Volume,
        volume: Some(v),
        mute: None,
        static_delay_ms: None,
    }
}

/// Queue `count` small audio frames whose payload encodes their index, so a test
/// can assert they arrive in push order.
fn enqueue_marked_audio(sender: &ServerSender, count: u8) {
    for i in 0..count {
        let frame = sendspin::server::encode_audio_frame(1_000 + i as i64, &[i; 8]);
        assert_eq!(
            sender.enqueue_audio(frame.into()).expect("enqueue"),
            AudioEnqueue::Sent
        );
    }
}

/// A player command shares the connection with audio but not the queue: volume,
/// mute and static delay take effect on arrival, so making them wait behind a
/// member's audio backlog only adds latency to a user-visible action.
///
/// The enqueues here are all synchronous with no `await` between them, so on the
/// single-threaded test runtime the writer task cannot have run yet: by the time
/// it does, both lanes have work waiting and the choice it makes is the thing
/// under test.
#[tokio::test]
async fn a_player_command_does_not_wait_behind_queued_audio() {
    let listener = ServerListener::bind("127.0.0.1:0", "test-server", "Test Server")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let peer = tokio::spawn({
        let url = format!("ws://{addr}");
        async move { connect_peer(&url, "member").await }
    });
    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut read = peer.await.unwrap();

    let sender = conn.sender();
    enqueue_marked_audio(&sender, 5);
    let queued_command = sender.queue_player_command(volume(42));
    queued_command.written().await.expect("command written");

    // The command overtook all five queued frames...
    let first = timeout(Duration::from_secs(5), read.next())
        .await
        .expect("timed out")
        .expect("no message")
        .unwrap();
    let text = match first {
        WsMessage::Text(t) => t,
        WsMessage::Binary(_) => {
            panic!("audio was written before the player command — control lane has no priority")
        }
        other => panic!("expected text, got {other:?}"),
    };
    match serde_json::from_str::<Message>(&text).unwrap() {
        Message::ServerCommand(cmd) => {
            assert_eq!(cmd.player.expect("player command").volume, Some(42))
        }
        other => panic!("expected server/command, got {other:?}"),
    }

    // ...and none of them were lost or reordered by being overtaken.
    for i in 0..5u8 {
        let frame = match timeout(Duration::from_secs(5), read.next())
            .await
            .expect("timed out")
            .expect("no message")
            .unwrap()
        {
            WsMessage::Binary(b) => b,
            other => panic!("expected audio frame {i}, got {other:?}"),
        };
        let chunk = AudioChunk::from_bytes(&frame).unwrap();
        assert_eq!(&*chunk.data, &[i; 8][..], "audio frames must stay in order");
    }
}

/// A socket that has stopped draining must not park control frames or the close
/// handshake forever.
///
/// The writer task is the only thing that touches the socket, so an unbounded
/// `sink.send` would hold everything behind it — a player command, `stream/end`,
/// the close command, and therefore `disconnect()`. Bounding each write makes a
/// stalled peer a dead connection instead: the write fails, the connection is
/// torn down, and callers get an error rather than waiting.
#[tokio::test]
async fn a_stalled_socket_fails_control_and_close_instead_of_hanging() {
    // 200ms rather than the 5s default so the test doesn't have to wait it out.
    let write_timeout = Duration::from_millis(200);
    let listener = ServerListener::bind("127.0.0.1:0", "test-server", "Test Server")
        .await
        .expect("bind")
        .write_timeout(write_timeout);
    let addr = listener.local_addr().expect("local_addr");
    let peer = tokio::spawn({
        let url = format!("ws://{addr}");
        async move { connect_peer(&url, "stalled-member").await }
    });
    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    // Hold the peer's read half without ever reading from it: the server's send
    // buffer and the peer's receive buffer fill up and stay full.
    let _read = peer.await.unwrap();

    // Push half-megabyte frames until the backlog is full. `Evicted` is proof the
    // writer is genuinely stuck on a write rather than merely slow — the queue
    // only reaches its cap if the frame at the head isn't completing.
    let big: Vec<u8> = vec![0xA5; 512 * 1024];
    let sender = conn.sender();
    let mut stalled = false;
    for _ in 0..200 {
        let frame = sendspin::server::encode_audio_frame(1_000, &big);
        match sender.enqueue_audio(frame.into()) {
            Ok(AudioEnqueue::Sent) => tokio::task::yield_now().await,
            Ok(AudioEnqueue::Evicted) => {
                stalled = true;
                break;
            }
            Err(e) => panic!("connection died before it could stall: {e}"),
        }
    }
    assert!(
        stalled,
        "expected a peer that never reads to back the writer up to its backlog cap"
    );

    // A control frame behind that stalled write must resolve, not hang. It fails
    // (the stalled write kills the connection), which is the correct outcome: the
    // member is unreachable and its Group prunes it.
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(10),
        sender.send_player_command(volume(30)),
    )
    .await;
    let elapsed = started.elapsed();
    assert!(
        result.is_ok(),
        "a control send stayed parked behind a stalled audio write"
    );
    assert!(
        result.unwrap().is_err(),
        "a control send onto a stalled socket must report failure, not success"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "control send took {elapsed:?}; expected roughly the {write_timeout:?} write deadline"
    );

    // And disconnect() — which waits for the close acknowledgement — returns too.
    assert!(
        timeout(Duration::from_secs(10), conn.disconnect())
            .await
            .is_ok(),
        "disconnect() hung on a stalled socket"
    );
}

/// Set up a listener plus one connected, non-reading peer and a single-member
/// group, returning the pieces the ordering tests need.
async fn one_member_group() -> (Group, sendspin::ServerConnection, PeerRead) {
    let listener = ServerListener::bind("127.0.0.1:0", "test-server", "Test Server")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let peer = tokio::spawn({
        let url = format!("ws://{addr}");
        async move { connect_peer(&url, "member").await }
    });
    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let read = peer.await.unwrap();
    let group = Group::new(Arc::new(sendspin::DefaultClock::default()));
    group
        .add_member(conn.client_id().to_string(), conn.sender())
        .await
        .unwrap();
    (group, conn, read)
}

/// What a client actually observed, reduced to the part these tests care about:
/// the order of lifecycle frames and audio payloads.
#[derive(Debug, PartialEq, Eq)]
enum Frame {
    /// `stream/start`, carrying the sample rate so a format change is visible.
    Start(u32),
    End,
    Clear,
    /// One audio frame, identified by its (uniform) first payload byte.
    Audio(u8),
}

/// Drain everything the peer has been sent, until the close frame.
async fn drain(read: &mut PeerRead) -> Vec<Frame> {
    let mut out = Vec::new();
    while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(5), read.next()).await {
        match msg {
            WsMessage::Text(text) => match serde_json::from_str::<Message>(&text).unwrap() {
                Message::StreamStart(s) => {
                    out.push(Frame::Start(s.player.expect("player config").sample_rate))
                }
                Message::StreamEnd(_) => out.push(Frame::End),
                Message::StreamClear(_) => out.push(Frame::Clear),
                _ => {}
            },
            WsMessage::Binary(bytes) => {
                let chunk = AudioChunk::from_bytes(&bytes).unwrap();
                out.push(Frame::Audio(chunk.data[0]));
            }
            WsMessage::Close(_) => break,
            _ => {}
        }
    }
    out
}

/// `stream/end` means "after everything I sent", so it must **not** overtake the
/// audio queued ahead of it — the naive fix for the starvation problem (give
/// control frames blanket priority) silently truncates the tail of every stream.
///
/// The pushes and the `end_stream` queueing happen with no `await` between them,
/// so on the single-threaded test runtime the writer cannot have drained anything
/// in between: when it runs, all five frames and the `stream/end` are queued
/// together and the order it produces is the thing under test.
#[tokio::test]
async fn audio_queued_before_stream_end_is_written_before_it() {
    let (group, conn, mut read) = one_member_group().await;
    group.start_stream(pcm_config()).await;
    for i in 0..5u8 {
        group.push_audio(&[i; 8]);
    }
    group.end_stream().await;
    conn.disconnect().await.unwrap();

    assert_eq!(
        drain(&mut read).await,
        vec![
            Frame::Start(48000),
            Frame::Audio(0),
            Frame::Audio(1),
            Frame::Audio(2),
            Frame::Audio(3),
            Frame::Audio(4),
            Frame::End,
        ],
        "stream/end must follow the audio pushed before it, in push order"
    );
}

/// A `stream/start` supersedes the stream before it, so audio still queued from
/// that old stream must be dropped rather than delivered after the new
/// `stream/start` — where the client would decode it against the *new* format.
/// Audio pushed after the restart is untouched.
#[tokio::test]
async fn audio_from_a_superseded_stream_is_dropped_at_the_next_stream_start() {
    let (group, conn, mut read) = one_member_group().await;
    group.start_stream(pcm_config()).await;
    // Queued against the 48kHz stream, then superseded before the writer runs.
    for _ in 0..5 {
        group.push_audio(&[0xAA; 8]);
    }
    let restarted = StreamPlayerConfig {
        sample_rate: 44100,
        ..pcm_config()
    };
    group.start_stream(restarted).await;
    group.push_audio(&[0xBB; 8]);
    // `disconnect` discards whatever audio is still queued, so park the read half
    // on an *awaited* chunk: the audio lane is FIFO, so once this one is on the
    // wire the 0xBB frame provably is too.
    conn.sender()
        .send_audio_chunk(9_999, &[0xCC; 8])
        .await
        .unwrap();
    conn.disconnect().await.unwrap();

    assert_eq!(
        drain(&mut read).await,
        vec![
            Frame::Start(48000),
            Frame::Start(44100),
            Frame::Audio(0xBB),
            Frame::Audio(0xCC),
        ],
        "audio from the superseded stream must not survive the restart"
    );
}

/// `stream/clear` tells the client to discard buffered audio; writing the audio
/// we're still holding for it would be pointless work at best and a race at
/// worst, so it's dropped one hop earlier.
#[tokio::test]
async fn audio_queued_before_stream_clear_is_dropped() {
    let (group, conn, mut read) = one_member_group().await;
    group.start_stream(pcm_config()).await;
    for _ in 0..5 {
        group.push_audio(&[0xAA; 8]);
    }
    group.clear_stream().await;
    group.push_audio(&[0xBB; 8]);
    // See the note in the superseded-stream test: awaited so the push above is
    // provably on the wire before `disconnect` drops what's left.
    conn.sender()
        .send_audio_chunk(9_999, &[0xCC; 8])
        .await
        .unwrap();
    conn.disconnect().await.unwrap();

    assert_eq!(
        drain(&mut read).await,
        vec![
            Frame::Start(48000),
            Frame::Clear,
            Frame::Audio(0xBB),
            Frame::Audio(0xCC),
        ],
        "audio the client is being told to discard must not be written"
    );
}

/// Lifecycle transitions and audio pushes contending from *different threads* —
/// the shape a real capture pipeline has, with a dedicated audio thread pushing
/// while the async side starts and ends streams.
///
/// The strong ordering guarantee here is structural: `Group` queues its
/// lifecycle frames and its audio under one lock, so there is no window inside
/// `start_stream`/`end_stream` for a push to interleave. That's not directly
/// observable from outside (a push that races the *call* is the caller's own
/// ordering, not the library's), so what this asserts is what contention could
/// still break: lifecycle frames must not be lost, duplicated, or reordered
/// relative to each other, and the connection must survive. Audio between
/// windows is the pusher's own doing and ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_frames_stay_paired_while_another_thread_pushes() {
    let (group, conn, mut read) = one_member_group().await;
    let group = Arc::new(group);

    let stop = Arc::new(AtomicBool::new(false));
    let pusher = std::thread::spawn({
        let group = Arc::clone(&group);
        let stop = Arc::clone(&stop);
        move || {
            while !stop.load(Ordering::Relaxed) {
                group.push_audio(&[0x11; 64]);
                std::thread::yield_now();
            }
        }
    });

    for _ in 0..5 {
        group.start_stream(pcm_config()).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        group.end_stream().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    stop.store(true, Ordering::Relaxed);
    pusher.join().unwrap();
    conn.disconnect().await.unwrap();

    let lifecycle: Vec<Frame> = drain(&mut read)
        .await
        .into_iter()
        .filter(|f| !matches!(f, Frame::Audio(_)))
        .collect();
    let expected: Vec<Frame> = (0..5)
        .flat_map(|_| [Frame::Start(48000), Frame::End])
        .collect();
    assert_eq!(
        lifecycle, expected,
        "lifecycle frames must stay strictly paired under cross-thread contention"
    );
}

/// A peer that floods `client/time` must not be able to convert its own send rate
/// into server memory, nor starve the audio lane.
///
/// Control frames are written ahead of audio, so answering every request would let
/// the peer keep the control lane permanently non-empty and the audio lane
/// permanently unpolled. Replies are therefore coalesced into a single slot (only
/// the newest request matters) and rate-limited, so a flood costs O(1) and audio
/// keeps flowing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_time_flood_neither_grows_nor_starves_the_audio_lane() {
    let listener = ServerListener::bind("127.0.0.1:0", "test-server", "Test Server")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    // The peer floods client/time as fast as it can while reading replies.
    let peer = tokio::spawn(async move {
        let (ws, _) = connect_async(format!("ws://{addr}"))
            .await
            .expect("ws connect");
        let (mut write, mut read) = ws.split();
        let hello = serde_json::to_string(&Message::ClientHello(test_hello("flooder"))).unwrap();
        write.send(WsMessage::Text(hello.into())).await.unwrap();
        read.next().await.expect("no server/hello").unwrap();

        let flood = tokio::spawn(async move {
            let mut sent = 0u32;
            let deadline = tokio::time::Instant::now() + Duration::from_millis(600);
            while tokio::time::Instant::now() < deadline {
                let msg = serde_json::to_string(&Message::ClientTime(
                    sendspin::protocol::messages::ClientTime {
                        client_transmitted: sent as i64,
                    },
                ))
                .unwrap();
                if write.send(WsMessage::Text(msg.into())).await.is_err() {
                    break;
                }
                sent += 1;
            }
            sent
        });

        let mut audio = 0u32;
        let mut replies = 0u32;
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(900), read.next()).await {
            match msg {
                WsMessage::Binary(_) => audio += 1,
                WsMessage::Text(_) => replies += 1,
                _ => {}
            }
        }
        (flood.await.unwrap(), audio, replies)
    });

    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let group = Group::new(Arc::new(sendspin::DefaultClock::default()));
    group
        .add_member(conn.client_id().to_string(), conn.sender())
        .await
        .unwrap();
    group.start_stream(pcm_config()).await;

    // Push audio for as long as the flood runs.
    for _ in 0..30 {
        group.push_audio(&[0x22; 256]);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(conn);

    let (sent, audio, replies) = peer.await.unwrap();
    assert!(
        sent > 100,
        "the flood needs to be a flood; only sent {sent}"
    );
    assert!(
        audio > 0,
        "audio was starved to zero by {sent} client/time requests"
    );
    assert!(
        replies < sent / 2,
        "{replies} replies for {sent} requests — they are not being coalesced"
    );
}

/// A connection whose writer has died must be reported as dead even when the audio
/// backlog happens to be full.
///
/// The backlog counter is only decremented by the writer, so frames still queued
/// when it exits are never accounted for — leaving the counter pegged at the cap on
/// a connection that is already gone. If the cap were checked before liveness, the
/// caller would be told `Evicted` forever and would never prune the member: no
/// audio, no `Disconnected`, no re-dial, nothing logged above `trace`.
#[tokio::test]
async fn a_dead_writer_is_reported_even_with_a_full_backlog() {
    let listener = ServerListener::bind("127.0.0.1:0", "test-server", "Test Server")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let peer = tokio::spawn({
        let url = format!("ws://{addr}");
        async move { connect_peer(&url, "member").await }
    });
    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let _read = peer.await.unwrap();
    let sender = conn.sender();

    // Fill the lane with no await in between, so the writer has not run: the cap is
    // reached with every frame still queued.
    let mut capped = false;
    for _ in 0..MAX_BACKLOG_PROBE {
        let frame = sendspin::server::encode_audio_frame(1, &[0u8; 64]);
        match sender.enqueue_audio(frame.into()).expect("enqueue") {
            AudioEnqueue::Sent => {}
            AudioEnqueue::Evicted => {
                capped = true;
                break;
            }
        }
    }
    assert!(capped, "expected the backlog to reach its cap");

    // Kill the writer through the close path, which drops the queued frames
    // without decrementing them.
    let _ = timeout(Duration::from_secs(5), conn.disconnect()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let frame = sendspin::server::encode_audio_frame(2, &[0u8; 8]);
    assert!(
        sender.enqueue_audio(frame.into()).is_err(),
        "a full backlog hid a dead writer — the member would never be pruned"
    );
}

/// A peer that completes the WebSocket handshake and then says nothing must not
/// park the task driving it, and must not hold up the next peer.
///
/// `accept` drives the handshake inline, so an unbounded `client/hello` read blocks
/// every subsequent inbound connection; on the dial side the same read parks a
/// supervisor with no backoff progression.
#[tokio::test]
async fn a_silent_peer_fails_its_handshake_without_blocking_the_next_one() {
    let listener = ServerListener::bind("127.0.0.1:0", "test-server", "Test Server")
        .await
        .expect("bind")
        .handshake_timeout(Duration::from_millis(300));
    let addr = listener.local_addr().expect("local_addr");

    // Connects, never sends client/hello, and stays connected.
    let silent = tokio::spawn(async move {
        let (ws, _) = connect_async(format!("ws://{addr}"))
            .await
            .expect("connect");
        tokio::time::sleep(Duration::from_secs(3)).await;
        drop(ws);
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let started = Instant::now();
    let first = timeout(Duration::from_secs(3), listener.accept()).await;
    assert!(
        first.is_ok(),
        "accept() never returned for a silent peer — the accept loop is wedged"
    );
    assert!(
        first.unwrap().is_err(),
        "a silent peer must fail its handshake"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "handshake bound not honoured: took {:?}",
        started.elapsed()
    );

    // The listener is still usable: a well-behaved peer connects right after.
    let good = tokio::spawn({
        let url = format!("ws://{addr}");
        async move { connect_peer(&url, "good-member").await }
    });
    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("second accept timed out")
        .expect("second accept failed");
    assert_eq!(conn.client_id(), "good-member");
    drop(good);
    silent.abort();
}

/// `stream/end` flushes the audio pushed before it, but that flush shares one
/// write-timeout budget with the frame itself.
///
/// Giving each flushed frame its own deadline would make the bound backlog ×
/// write_timeout — with the defaults, minutes — for a member that is merely slow
/// rather than dead, because every individual write succeeds. Past the budget the
/// remaining tail is dropped instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_end_stays_bounded_against_a_slow_peer() {
    let write_timeout = Duration::from_millis(300);
    let listener = ServerListener::bind("127.0.0.1:0", "test-server", "Test Server")
        .await
        .expect("bind")
        .write_timeout(write_timeout);
    let addr = listener.local_addr().expect("local_addr");

    // Drains one message per 120ms: never stalls a single write, but far too slow
    // to clear a full backlog inside one timeout.
    let peer = tokio::spawn(async move {
        let (ws, _) = connect_async(format!("ws://{addr}"))
            .await
            .expect("connect");
        let (mut write, mut read) = ws.split();
        let hello = serde_json::to_string(&Message::ClientHello(test_hello("slow"))).unwrap();
        write.send(WsMessage::Text(hello.into())).await.unwrap();
        read.next().await.unwrap().unwrap();
        loop {
            tokio::time::sleep(Duration::from_millis(120)).await;
            if timeout(Duration::from_secs(2), read.next()).await.is_err() {
                break;
            }
        }
    });
    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let sender = conn.sender();

    let big = vec![0x5Au8; 256 * 1024];
    for _ in 0..MAX_BACKLOG_PROBE {
        let frame = sendspin::server::encode_audio_frame(1, &big);
        if matches!(
            sender.enqueue_audio(frame.into()),
            Ok(AudioEnqueue::Evicted) | Err(_)
        ) {
            break;
        }
    }

    let started = Instant::now();
    let _ = timeout(Duration::from_secs(20), sender.send_stream_end()).await;
    let elapsed = started.elapsed();
    assert!(
        elapsed < 4 * write_timeout,
        "stream/end took {elapsed:?}; the flush is paying per-frame deadlines rather \
         than one shared budget"
    );
    peer.abort();
}
