use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:9000").await?;
    let topics = vec!["t1", "t2", "t3"];
    let partitions: Vec<u32> = vec![1, 2, 3, 4];
    for topic in topics {
        for part in &partitions {
            for i in 0..5 {
                let msg = format!(
                    r#"{{"cmd":"produce","topic":"{}","partition":{},"msg":"hello from producer {} (topic={} partition={})"}}"#,
                    topic, part, i, topic, part
                );
                stream.write_all(msg.as_bytes()).await?;
                stream.write_all(b"\n").await?;

                let mut reader = BufReader::new(&mut stream).lines();
                if let Some(line) = reader.next_line().await? {
                    println!("Broker response: {}", line);
                }
            }
        }
    }
    Ok(())
}
