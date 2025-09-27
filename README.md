# mini-kafka
A simple Kafka-like message broker written in Rust for learning purposes.
Supports multiple topics, partitions, message persistence, and message metadata (ID, timestamp, value).

## 🚀 Features
- Multiple topics and partitions
- Persistent storage (messages survive broker restarts)
- Message metadata (unique ID, timestamp, value)
- Simple JSON-based protocol
- Producer and consumer clients

## 🛠️ Getting Started
1. Clone and Build
```
git clone <your-repo-url>
cd mini-kafka
cargo build
```

Then, open multiple terminal tabs for each step below.

2. Run the Broker
```
RUST_LOG=info BROKER_ID=0 cargo run --bin mini-kafka
```
- Each broker instance should have a unique `BROKER_ID` (0, 1, 2, ...).
- Program will read configuration from `broker_config.json`.

3. Produce Messages
```
echo '{"cmd":"produce","topic":"foo","partition":0,"msg":"hello world"}' | nc 127.0.0.1 9000
```

4. Consume Messages
```
echo '{"cmd":"consume","topic":"foo","partition":0,"offset":0}' | nc 127.0.0.1 9000
```

## 📄 Protocol Example
Produce:
```json
{"cmd":"produce","topic":"foo","partition":0,"msg":"hello"}
```

Consume:
```json
{"cmd":"consume","topic":"foo","partition":0,"offset":0}
```

## 📝 Notes
- Messages are stored as JSON lines in files named topic_<topic>_part_<partition>.txt.
- Each message includes a unique ID and timestamp.
- This project is for educational purposes and is not production-ready.

## 🙏 Credits
Inspired by Kafka and CodeCrafters.
