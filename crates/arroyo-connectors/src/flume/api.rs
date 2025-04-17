use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::io::BufRead;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub struct TopicId(String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub id: TopicId,
    pub name: String,
    pub partition_count: usize,
    pub retention_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    id: usize,
    high_watermark: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMetadata {
    timestamp: i64,
    headers: HashMap<String, String>,
}

#[derive(Debug, Clone)] // Serialize and Deserialize defined manually
pub struct Message {
    pub partition: usize,
    pub offset: usize,
    pub payload: Vec<u8>,
    pub metadata: MessageMetadata,
}

// Custom serializer implementation to handle base64 encoding automatically
impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        // Convert the binary payload to base64
        let payload_base64 = BASE64.encode(&self.payload);

        // Create a struct with 3 fields
        let mut state = serializer.serialize_struct("Message", 3)?;

        // Serialize the fields
        state.serialize_field("offset", &self.offset)?;
        state.serialize_field("payload", &payload_base64)?;
        state.serialize_field("metadata", &self.metadata)?;

        state.end()
    }
}

// Custom deserializer implementation to handle base64 decoding automatically
impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawMessage {
            partition: usize,
            offset: usize,
            payload: String,
            metadata: MessageMetadata,
        }

        let raw = RawMessage::deserialize(deserializer)?;

        let payload = BASE64
            .decode(&raw.payload)
            .map_err(|e| de::Error::custom(format!("Failed to decode base64: {}", e)))?;

        Ok(Message {
            partition: raw.partition,
            offset: raw.offset,
            metadata: raw.metadata,
            payload,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageResponse {
    pub partition: usize,
    pub high_watermark: usize,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone)]
pub struct ApiClient {
    endpoint: String,
    client: reqwest::Client,
}

impl ApiClient {
    pub fn new(endpoint: String, client: reqwest::Client) -> Self {
        Self { endpoint, client }
    }

    pub async fn list_topics(&self) -> anyhow::Result<Vec<TopicInfo>> {
        let info = self
            .client
            .get(format!("{}/topics", self.endpoint))
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<TopicInfo>>()
            .await?;

        Ok(info)
    }

    pub async fn get_topic(&self, topic_name: &str) -> anyhow::Result<TopicInfo> {
        let info = self
            .client
            .get(format!("{}/topics/{}", self.endpoint, topic_name))
            .send()
            .await?
            .error_for_status()?
            .json::<TopicInfo>()
            .await?;

        Ok(info)
    }

    pub async fn get_messages(
        &self,
        topic_name: &str,
        partition_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> anyhow::Result<MessageResponse> {
        let url = format!(
            "{}/topics/{}/partitions/{}/messages?offset={}&maxMessages={}",
            self.endpoint, topic_name, partition_id, offset, batch_size
        );

        info!("Fetching messages for {}", url);

        let resp = self.client.get(url).send().await?.error_for_status()?;

        let hwm = resp
            .headers()
            .get("X-High-Water-Mark")
            .map(|value| value.to_str().unwrap().parse::<usize>().unwrap())
            .expect("should always have X-High-Water-Mark header");

        // Read NDJSON
        let mut messages = vec![];

        // Create an in-memory reader from the downloaded bytes
        // TODO: Do it as stream??
        let cursor = std::io::Cursor::new(resp.bytes().await?);
        let reader = std::io::BufReader::new(cursor);

        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                let data: Message = serde_json::from_str(&line)?;
                messages.push(data);
            }
        }

        info!(
            "Got {} messages from partition {}",
            messages.len(),
            partition_id
        );

        Ok(MessageResponse {
            partition: partition_id,
            high_watermark: hwm,
            messages,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_topic_info() {
        let raw = r#"{"id":"09c70696-de94-495e-b77c-8f3241d0c3b3","name":"test","partition_count":15,"retention_seconds":300,"status":"ACTIVE","created_at":"2025-04-01T21:35:09.310Z","updated_at":"2025-04-02T20:57:25.958Z"}"#;

        let topic_info: TopicInfo = serde_json::from_str(raw).unwrap();
        assert_eq!(
            topic_info.id,
            TopicId(String::from("09c70696-de94-495e-b77c-8f3241d0c3b3"))
        );
        assert_eq!(topic_info.name, "test");
        assert_eq!(topic_info.partition_count, 15);
    }

    #[test]
    fn test_serde_topic_partition_info() {
        let raw = r#"{"id":1,"high_watermark":1318765,"retention_seconds":300,"percent_full":0.000024576}"#;

        let partition_info: PartitionInfo = serde_json::from_str(raw).unwrap();
        assert_eq!(partition_info.id, 1);
        assert_eq!(partition_info.high_watermark, 1318765);
    }

    #[test]
    fn test_serde_message() {
        let raw = r#"{"offset":1318687,"payload":"c28geW91J3ZlIGZvdW5kIG1lLi4uCg==","metadata":{"timestamp":1744760398,"headers":{"source":"k6-test","contentType":"text/plain","timestamp":"1744760396632"}}}"#;

        let message: Message = serde_json::from_str(raw).unwrap();
        assert_eq!(message.offset, 1318687);
        assert_eq!(message.metadata.timestamp, 1744760398);
    }

    #[tokio::test]
    async fn test_get_topic() {
        let client = ApiClient::new(
            "https://flume-cole.exmaple.xyz/api/accounts/123".to_string(),
            reqwest::ClientBuilder::default().build().unwrap(),
        );

        let topic = client.get_topic("test").await.unwrap();
        println!("{:?}", topic)
    }

    #[tokio::test]
    async fn test_list_topics() {
        let client = ApiClient::new(
            "https://flume-cole.exmaple.xyz/api/accounts/123".to_string(),
            reqwest::ClientBuilder::default().build().unwrap(),
        );

        let topics = client.list_topics().await.unwrap();
        println!("{:?}", topics)
    }
}
