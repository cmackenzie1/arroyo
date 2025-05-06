use crate::timber::api::{ApiClient, TopicInfo};
use crate::EmptyConfig;
use anyhow::{anyhow, bail};
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::{ConstructedOperator, SourceOperator};
use arroyo_operator::SourceFinishType;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::formats::{Format, Framing, JsonFormat, RawStringFormat};
use arroyo_rpc::grpc::rpc::StopMode;
use arroyo_rpc::{ConnectorOptions, ControlMessage, OperatorConfig};
use arroyo_types::UserError;
use async_trait::async_trait;
use fluvio::Offset;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{error, info};
use typify::import_types;

mod api;
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("../fluvio/fluvio.svg");

import_types!(schema = "src/timber/table.json");

pub struct TimberConnector {}

impl Connector for TimberConnector {
    type ProfileT = EmptyConfig;
    type TableT = FlumeTable;

    fn name(&self) -> &'static str {
        "timber"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "timber".to_string(),
            name: "Flume".to_string(),
            icon: ICON.to_string(),
            description: "Read and write from a Timber endpoint".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, t: Self::TableT) -> ConnectionType {
        match t.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Source,
        }
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let endpoint = options.pull_opt_str("endpoint")?;
        let topic = options.pull_str("topic")?;
        let table_type = options.pull_str("type")?;

        let table_type = match table_type.as_str() {
            "source" => {
                let offset = options.pull_opt_str("source.offset")?;
                TableType::Source {
                    offset: match offset.as_deref() {
                        Some("earliest") => SourceOffset::Earliest,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                }
            }
            "sink" => TableType::Sink {},
            _ => {
                bail!("type must be one of 'source' or 'sink'");
            }
        };

        let table = TimberTable {
            endpoint,
            topic,
            type_: table_type,
        };

        Self::from_config(self, None, name, EmptyConfig {}, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                format!("TimberSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => {
                (ConnectionType::Sink, format!("TimberSink<{}>", table.topic))
            }
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for Timber connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Timber connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            typ,
            schema,
            &config,
            desc,
        ))
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        match table.type_ {
            TableType::Source { offset } => {
                Ok(ConstructedOperator::from_source(Box::new(TimberFunc {
                    topic: table.topic,
                    api_client: ApiClient::new(table.endpoint.unwrap(), reqwest::Client::default()),
                })))
            }
            TableType::Sink { .. } => bail!("invalid type for sink operator"),
        }
    }
}

pub struct TimberFunc {
    topic: String,
    api_client: ApiClient,
}

impl TimberFunc {
    pub fn new(topic: String, api_client: ApiClient) -> Self {
        Self { topic, api_client }
    }
}

#[async_trait]
impl SourceOperator for TimberFunc {
    fn name(&self) -> String {
        format!("timber-{}", self.topic)
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        collector.initialize_deserializer(Format::RawString(RawStringFormat {}), None, None, &[]);
        let topic_info: TopicInfo = self.api_client.get_topic(&self.topic).await.unwrap();

        let mut offsets = HashMap::new();

        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut futures = FuturesUnordered::new();
        for partition in 0..topic_info.partition_count {
            offsets.insert(partition, 0);
            futures.push(
                self.api_client
                    .get_messages(&self.topic, partition, 0, 1000),
            );
        }

        loop {
            select! {
                result = futures.select_next_some() => {
                    match result {
                        Ok(messages) => {
                            let partition = messages.partition;

                            for message in messages.messages {
                                collector.deserialize_slice(message.payload.as_slice(), SystemTime::now(), None).await.unwrap();
                                 if collector.should_flush() {
                                    collector.flush_buffer().await.unwrap();
                                }
                                offsets.insert(partition, message.offset);
                            }

                            let offset = offsets.get(&partition).unwrap();
                             // fetch the next batch for that partition
                            futures.push(self.api_client.get_messages(
                                &self.topic,
                                partition,
                                *offset,
                                1000,
                            ));
                        },
                        Err(e) => {
                            error!("error consuming timber message: {}", e);
                        },
                    }
                },
                _ = flush_ticker.tick() => {
                    if collector.should_flush() {
                        collector.flush_buffer().await.unwrap();
                    }
                },
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping timber source: {:?}", mode);

                            return match mode {
                                StopMode::Graceful => {
                                    SourceFinishType::Graceful
                                }
                                StopMode::Immediate => {
                                    SourceFinishType::Immediate
                                }
                            };
                        }
                        Some(msg) => {
                            info!("Unhandled control message: {:?}", msg);
                        }
                        None => {}
                    }
                }
            }
        }
    }
}

impl SourceOffset {
    pub fn offset(&self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::beginning(),
            SourceOffset::Latest => Offset::end(),
        }
    }
}
