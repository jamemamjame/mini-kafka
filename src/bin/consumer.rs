use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:9000").await?;
    let msg = r#"{"cmd":"consume","topic":"t3","partition":2,"offset":3}"#;
    stream.write_all(msg.as_bytes()).await?;
    stream.write_all(b"\n").await?;

    let mut reader = BufReader::new(&mut stream).lines();
    if let Some(line) = reader.next_line().await? {
        println!("Broker response: {}", line);
    }
    Ok(())
}
