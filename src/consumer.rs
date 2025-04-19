use std::sync::LazyLock;

use serde::{Deserialize, de::DeserializeOwned};
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;
use zstd::dict::DecoderDictionary;

use crate::connection::{Connection, Cursor};

#[derive(Debug, Error)]
pub enum Error<E> {
    #[error("event JSON deserialization: {0}")]
    JsonEventDeserialize(serde_json::Error),
    #[error("compression decoder: {0}")]
    CompressDecoder(std::io::Error),
    #[error("compressed event JSON deserialization: {0}")]
    JsonCompressedEventDeserialize(serde_json::Error),
    #[error("non-commit event found when commit event expected")]
    NonCommitEvent,
    #[error("consumer: {0}")]
    Consumer(E),
}

#[derive(Debug, Deserialize)]
pub struct Event {
    pub did: String,
    pub time_us: u64,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Commit(CommitEvent),
    Identity {},
    Account {},
}

#[derive(Debug, Deserialize)]
pub struct CommitEvent {
    pub rev: String,
    pub operation: String,
    pub collection: String,
    pub rkey: String,
    pub record: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct FlattenedCommitEvent<T> {
    pub did: String,
    pub time_us: u64,
    pub rev: String,
    pub operation: String,
    pub collection: String,
    pub rkey: String,
    pub record: T,
}

impl Event {
    /// Returns the collection for the underlying `Commit` variant of the event
    ///
    /// Fails if `self` is not a `Commit` event.
    pub fn collection<E>(&self) -> Result<&str, Error<E>> {
        match &self.kind {
            EventKind::Commit(ce) => Ok(ce.collection.as_str()),
            _ => Err(Error::NonCommitEvent),
        }
    }

    /// Flattens the `Commit` part of the event into a composite structure and deserializes the
    /// `serde_json::Value` record into the appropriate type.
    ///
    /// Fails if `self` is not a `Commit` event or if deserialization fails.
    pub fn flatten<R, E>(self) -> Result<FlattenedCommitEvent<R>, Error<E>>
    where
        R: DeserializeOwned,
    {
        let Event {
            did,
            time_us,
            kind:
                EventKind::Commit(CommitEvent {
                    rev,
                    operation,
                    collection,
                    rkey,
                    record,
                }),
        } = self
        else {
            return Err(Error::NonCommitEvent);
        };

        Ok(FlattenedCommitEvent {
            did,
            time_us,
            rev,
            operation,
            collection,
            rkey,
            record: serde_json::from_value(record).map_err(Error::JsonEventDeserialize)?,
        })
    }
}

pub trait Consumer<R, E> {
    fn consume(&self, message: FlattenedCommitEvent<R>) -> impl Future<Output = Result<(), E>>;
}

pub trait MultiConsumer<E> {
    fn consume(&self, message: Event) -> impl Future<Output = Result<bool, Error<E>>> + Send;
}

// re-export so we can use inside the macro
pub use paste;

#[macro_export]
macro_rules! multi_consumer {
    ($visibility:vis $mconsumer:ident<$error:ty> { $($name:pat => $record:ty => $consumer_ty:ty = $consumer:expr),* }) => {{
        $crate::consumer::paste::paste! {

$visibility struct $mconsumer {$(
    [<consumer_ $record:snake>]: $consumer_ty
),*}

impl $crate::consumer::MultiConsumer<$error> for $mconsumer {
    async fn consume(&self, event: $crate::consumer::Event) -> Result<bool, $crate::consumer::Error<$error>> {
        match event.collection()? {
            $(
                $name => {
                    Self::[<consume_ $record:snake>](
                        event.flatten()?,
                        &self.[<consumer_ $record:snake>]
                    ).await.map_err($crate::consumer::Error::Consumer)?;
                    Ok(true)
                },
            )*
            _ => Ok(false)
        }
    }
}

impl $mconsumer {$(
    async fn [<consume_ $record:snake>](event: FlattenedCommitEvent<$record>, consumer: &$consumer_ty) -> Result<(), $error> {
        consumer.consume(event).await
    }
)*}

$mconsumer {$(
    [<consumer_ $record:snake>]: $consumer
),*}

        }
    }};
}

static ZSTD_DICTIONARY: LazyLock<DecoderDictionary> =
    LazyLock::new(|| DecoderDictionary::copy(include_bytes!("../zstd_dictionary")));

pub enum ProcessEffect {
    Closed(Option<String>),
    ProcessedCommit,
    ProcessedAccount,
    ProcessedIdentity,
    Ignored,
}

pub async fn process_message<C, E>(
    multi_consumer: &C,
    message: Message,
) -> Result<ProcessEffect, Error<E>>
where
    C: MultiConsumer<E>,
{
    let event = match message {
        Message::Text(text) => {
            serde_json::from_str::<Event>(&text).map_err(Error::JsonEventDeserialize)
        }
        Message::Binary(data) => {
            let decoder = zstd::Decoder::with_prepared_dictionary(
                std::io::Cursor::new(data),
                &*ZSTD_DICTIONARY,
            )
            .map_err(Error::CompressDecoder)?;
            serde_json::from_reader::<_, Event>(decoder)
                .map_err(Error::JsonCompressedEventDeserialize)
        }
        Message::Close(close) => {
            return Ok(ProcessEffect::Closed(close.map(|cf| cf.to_string())));
        }
        _ => {
            return Ok(ProcessEffect::Ignored);
        }
    }?;

    Ok(match event.kind {
        EventKind::Account {} => ProcessEffect::ProcessedAccount,
        EventKind::Identity {} => ProcessEffect::ProcessedIdentity,
        EventKind::Commit(_) => {
            multi_consumer.consume(event).await?;
            ProcessEffect::ProcessedCommit
        }
    })
}

pub async fn start_processor<C, E: std::error::Error>(
    multi_consumer: C,
    mut connection: Connection,
    cursor: Cursor,
) where
    C: MultiConsumer<E> + Send + Sync + 'static,
{
    let mut message_rx = connection
        .take_message_rx()
        .expect("message_rx already taken");
    tokio::spawn(async move {
        while let Some(message) = message_rx.recv().await {
            match process_message(&multi_consumer, message).await {
                Err(e) => {
                    eprintln!("error during message processing: {e}");
                }
                Ok(ProcessEffect::Closed(err_message)) => {
                    eprintln!(
                        "Jetstream connection closed{}",
                        err_message
                            .map(|em| format!(": {}", em.to_string()))
                            .unwrap_or("".to_owned())
                    );
                    break;
                }
                Ok(
                    ProcessEffect::Ignored
                    | ProcessEffect::ProcessedAccount
                    | ProcessEffect::ProcessedIdentity
                    | ProcessEffect::ProcessedCommit,
                ) => {}
            }
        }
    });

    if let Err(e) = connection.connect(cursor).await {
        eprintln!("Failed to connect to Jetstream: {}", e);
        std::process::exit(1);
    }
}
