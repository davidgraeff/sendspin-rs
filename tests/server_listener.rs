// ABOUTME: Integration tests for the server role's ServerListener — inbound
// ABOUTME: WebSocket acceptor that drives the protocol-server state machine.

use futures_util::{SinkExt, StreamExt};
use sendspin::protocol::messages::{
    ClientHello, ClientState, ClientSyncState, ClientTime, Message, StreamPlayerConfig,
};
use sendspin::server::ServerRole;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

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

#[tokio::test]
async fn accept_drives_handshake_and_grants_player_role() {
    let listener = ServerRole::new("test-server", "Test Server")
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("ws://{addr}");

    let peer = tokio::spawn(async move {
        let (ws, _) = connect_async(&url).await.expect("ws connect");
        let (mut write, mut read) = ws.split();

        let hello = serde_json::to_string(&Message::ClientHello(test_hello("peer-1"))).unwrap();
        write.send(WsMessage::Text(hello.into())).await.unwrap();

        let text = match read.next().await.expect("no server/hello").unwrap() {
            WsMessage::Text(t) => t,
            other => panic!("expected text server/hello, got {other:?}"),
        };
        match serde_json::from_str::<Message>(&text).expect("server/hello must deserialize") {
            Message::ServerHello(hello) => hello,
            other => panic!("expected server/hello, got {other:?}"),
        }
    });

    let (conn, peer_addr) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("accept timed out")
        .expect("accept failed");
    assert!(peer_addr.ip().is_loopback());
    assert_eq!(conn.client_id(), "peer-1");
    assert_eq!(conn.active_roles(), ["player@v1".to_string()]);

    let server_hello = peer.await.expect("peer task panicked");
    assert_eq!(server_hello.server_id, "test-server");
    assert_eq!(server_hello.active_roles, vec!["player@v1".to_string()]);
}

#[tokio::test]
async fn client_without_player_role_gets_no_active_roles() {
    let listener = ServerRole::new("test-server", "Test Server")
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("ws://{addr}");

    let peer = tokio::spawn(async move {
        let (ws, _) = connect_async(&url).await.expect("ws connect");
        let (mut write, mut read) = ws.split();
        let mut hello = test_hello("peer-2");
        hello.supported_roles = vec!["controller@v1".to_string()];
        let hello = serde_json::to_string(&Message::ClientHello(hello)).unwrap();
        write.send(WsMessage::Text(hello.into())).await.unwrap();
        read.next().await.expect("no server/hello").unwrap()
    });

    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert!(conn.active_roles().is_empty());
    peer.await.unwrap();
}

#[tokio::test]
async fn time_sync_echo_reflects_client_transmitted_and_orders_timestamps() {
    let listener = ServerRole::new("test-server", "Test Server")
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("ws://{addr}");

    let peer = tokio::spawn(async move {
        let (ws, _) = connect_async(&url).await.expect("ws connect");
        let (mut write, mut read) = ws.split();
        let hello = serde_json::to_string(&Message::ClientHello(test_hello("peer-3"))).unwrap();
        write.send(WsMessage::Text(hello.into())).await.unwrap();
        read.next().await.expect("no server/hello").unwrap(); // discard server/hello

        let client_transmitted = 1_000_000i64;
        let msg =
            serde_json::to_string(&Message::ClientTime(ClientTime { client_transmitted })).unwrap();
        write.send(WsMessage::Text(msg.into())).await.unwrap();

        let text = match read.next().await.expect("no server/time").unwrap() {
            WsMessage::Text(t) => t,
            other => panic!("expected text server/time, got {other:?}"),
        };
        match serde_json::from_str::<Message>(&text).expect("server/time must deserialize") {
            Message::ServerTime(st) => st,
            other => panic!("expected server/time, got {other:?}"),
        }
    });

    let (_conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();

    let server_time = timeout(Duration::from_secs(5), peer)
        .await
        .expect("peer task timed out")
        .expect("peer task panicked");

    assert_eq!(server_time.client_transmitted, 1_000_000);
    assert!(
        server_time.server_transmitted >= server_time.server_received,
        "server_transmitted ({}) must not precede server_received ({}) — it's stamped \
         immediately before the reply goes on the wire, strictly after receipt",
        server_time.server_transmitted,
        server_time.server_received
    );
}

#[tokio::test]
async fn every_client_time_is_answered_even_back_to_back() {
    // Regression guard. This server used to drop any `client/time` arriving within
    // 50 ms of its previous reply, justified as "the spec's cadence is about one
    // per second". The spec says the opposite — cadence is the client's choice, and
    // the reference implementation it points at sends a burst of 8 back-to-back,
    // each waiting for its reply.
    //
    // The cost on real hardware was severe and took a long investigation to find: an
    // ESPHome player is one-request-in-flight with a 10 s response timeout and no
    // retransmit, and it decodes *nothing* until its first burst completes. Every
    // dropped request therefore bought 10 s of total silence, and a reconnect cost
    // 20-30 s — varying per device with how its main loop happened to straddle the
    // 50 ms window, which is what made it look like a per-device firmware bug.
    //
    // So: a burst of requests with no pacing whatsoever must produce exactly as many
    // `server/time` replies, each echoing its own `client_transmitted`. If a future
    // change adds backpressure here it must still *reply* to every request.
    const BURST: usize = 8;

    let listener = ServerRole::new("test-server", "Test Server")
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("ws://{addr}");

    let peer = tokio::spawn(async move {
        let (ws, _) = connect_async(&url).await.expect("ws connect");
        let (mut write, mut read) = ws.split();
        let hello = serde_json::to_string(&Message::ClientHello(test_hello("peer-burst"))).unwrap();
        write.send(WsMessage::Text(hello.into())).await.unwrap();
        read.next().await.expect("no server/hello").unwrap(); // discard server/hello

        // Send the whole burst first, with no delay between sends: this is the case
        // the old rate limit silently discarded.
        for i in 0..BURST {
            let client_transmitted = 1_000_000i64 + i as i64;
            let msg = serde_json::to_string(&Message::ClientTime(ClientTime { client_transmitted }))
                .unwrap();
            write.send(WsMessage::Text(msg.into())).await.unwrap();
        }

        let mut echoed = Vec::new();
        for _ in 0..BURST {
            let text = match read.next().await.expect("server closed early").unwrap() {
                WsMessage::Text(t) => t,
                other => panic!("expected text server/time, got {other:?}"),
            };
            match serde_json::from_str::<Message>(&text).expect("server/time must deserialize") {
                Message::ServerTime(st) => echoed.push(st.client_transmitted),
                other => panic!("expected server/time, got {other:?}"),
            }
        }
        echoed
    });

    let (_conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();

    let echoed = timeout(Duration::from_secs(10), peer)
        .await
        .expect("peer timed out waiting for replies — a request was dropped")
        .expect("peer task panicked");

    let expected: Vec<i64> = (0..BURST).map(|i| 1_000_000i64 + i as i64).collect();
    assert_eq!(
        echoed, expected,
        "every client/time must get its own server/time back, in order"
    );
}

#[tokio::test]
async fn pushed_audio_and_stream_lifecycle_reach_the_client_intact() {
    let listener = ServerRole::new("test-server", "Test Server")
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("ws://{addr}");

    let peer = tokio::spawn(async move {
        let (ws, _) = connect_async(&url).await.expect("ws connect");
        let (mut write, mut read) = ws.split();
        let hello = serde_json::to_string(&Message::ClientHello(test_hello("peer-4"))).unwrap();
        write.send(WsMessage::Text(hello.into())).await.unwrap();
        read.next().await.expect("no server/hello").unwrap();

        let state = serde_json::to_string(&Message::ClientState(ClientState {
            state: Some(ClientSyncState::Synchronized),
            player: None,
        }))
        .unwrap();
        write.send(WsMessage::Text(state.into())).await.unwrap();

        // stream/start
        let text = match read.next().await.expect("no stream/start").unwrap() {
            WsMessage::Text(t) => t,
            other => panic!("expected text stream/start, got {other:?}"),
        };
        assert!(matches!(
            serde_json::from_str::<Message>(&text).unwrap(),
            Message::StreamStart(_)
        ));

        // one binary audio frame
        let frame = match read.next().await.expect("no audio frame").unwrap() {
            WsMessage::Binary(b) => b,
            other => panic!("expected binary audio frame, got {other:?}"),
        };
        let chunk = sendspin::protocol::client::AudioChunk::from_bytes(&frame)
            .expect("must parse as a player audio chunk");

        // stream/end
        let text = match read.next().await.expect("no stream/end").unwrap() {
            WsMessage::Text(t) => t,
            other => panic!("expected text stream/end, got {other:?}"),
        };
        assert!(matches!(
            serde_json::from_str::<Message>(&text).unwrap(),
            Message::StreamEnd(_)
        ));

        chunk
    });

    let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let sender = conn.sender();

    sender
        .queue_stream_start(StreamPlayerConfig {
            codec: "pcm".to_string(),
            sample_rate: 44100,
            channels: 2,
            bit_depth: 16,
            codec_header: None,
        })
        .await
        .expect("send_stream_start");
    let payload = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    sender
        .send_audio_chunk(42_000, &payload)
        .await
        .expect("send_audio_chunk");
    sender.queue_stream_end().await.expect("stream/end");

    let chunk = timeout(Duration::from_secs(5), peer)
        .await
        .expect("peer task timed out")
        .expect("peer task panicked");
    assert_eq!(chunk.timestamp, 42_000);
    assert_eq!(&*chunk.data, &payload[..]);
}
