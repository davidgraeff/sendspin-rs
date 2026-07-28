// ABOUTME: Integration tests for the per-connection writer's two lanes: control
// ABOUTME: frames never wait behind queued audio, and a socket that has stopped
// ABOUTME: draining fails its writes instead of parking control and close forever.

use futures_util::{SinkExt, StreamExt};
use sendspin::protocol::client::AudioChunk;
use sendspin::protocol::messages::{ClientHello, Message, PlayerCommand, PlayerCommandType};
use sendspin::server::{AudioEnqueue, ServerSender};
use sendspin::ServerListener;
use std::time::{Duration, Instant};
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
