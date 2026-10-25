use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub const BODY: &[u8] = b"123456789";
pub const BODY_CRC: u32 = 0x2dfd_2d88;

pub struct Responder {
    pub url: String,
    hits: Arc<AtomicUsize>,
}

impl Responder {
    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

pub async fn respond(status: &'static str, body: &'static [u8]) -> Responder {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.expect("accept");
            counter.fetch_add(1, Ordering::SeqCst);
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let head = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.expect("write head");
            socket.write_all(body).await.expect("write body");
        }
    });
    Responder {
        url: format!("http://{addr}"),
        hits,
    }
}
