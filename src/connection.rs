use futures_util::StreamExt;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

#[derive(Debug, Error)]
pub enum Error {
    #[error("URL parse: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("WSS connect: {0}")]
    WssConnect(tokio_tungstenite::tungstenite::Error),
    #[error("jetstream connection dropped")]
    ConnectionDropped,
    #[error("jetstream read: {0}")]
    JetstreamRead(tokio_tungstenite::tungstenite::Error),
    #[error("mpsc message send: {0}")]
    MessageSend(#[from] tokio::sync::mpsc::error::SendError<Message>),
}

pub mod bluesky_instances {
    pub const US_EAST_1: &'static str = "jetstream1.us-east.bsky.network";
    pub const US_EAST_2: &'static str = "jetstream2.us-east.bsky.network";
    pub const US_WEST_1: &'static str = "jetstream1.us-west.bsky.network";
    pub const US_WEST_2: &'static str = "jetstream2.us-west.bsky.network";
}

pub struct Options {
    hostname: String,
    channel_size: usize,
    max_message_size_bytes: usize,
    compress: bool,
    wanted_collections: Vec<String>,
    wanted_dids: Vec<String>,
}

impl Options {
    pub fn new(hostname: impl AsRef<str>) -> Self {
        Self {
            hostname: hostname.as_ref().to_owned(),
            channel_size: 2usize << 16,                  // default 2^16
            max_message_size_bytes: 20 * (2usize << 20), // default 20 MB
            compress: true,
            wanted_collections: vec![],
            wanted_dids: vec![],
        }
    }

    pub fn channel_size(mut self, channel_size: usize) -> Self {
        self.channel_size = channel_size;
        self
    }

    pub fn max_message_size_bytes(mut self, max_message_size_bytes: usize) -> Self {
        self.max_message_size_bytes = max_message_size_bytes;
        self
    }

    pub fn compress(mut self, compress: bool) -> Self {
        self.compress = compress;
        self
    }

    pub fn wanted_collections(mut self, collections: impl IntoIterator<Item = String>) -> Self {
        self.wanted_collections.extend(collections);
        self
    }

    pub fn wanted_dids(mut self, dids: impl IntoIterator<Item = String>) -> Self {
        self.wanted_dids.extend(dids);
        self
    }
}

pub struct Connection {
    message_tx: Sender<Message>,
    message_rx: Option<Receiver<Message>>,
    options: Options,
}

// a newtype just because this will probably need to be atomic eventually (to keep cursor updated
// with message timestamps)
pub struct Cursor(u64);

impl From<u64> for Cursor {
    fn from(value: u64) -> Self {
        Cursor(value)
    }
}

impl Cursor {
    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn get_mut(&mut self) -> &mut u64 {
        &mut self.0
    }
}

impl Connection {
    pub fn new(options: Options) -> Self {
        let (message_tx, message_rx) = channel(options.channel_size);
        Self {
            message_rx: Some(message_rx),
            message_tx,
            options,
        }
    }

    pub fn take_message_rx(&mut self) -> Option<Receiver<Message>> {
        self.message_rx.take()
    }

    fn wss_url(&self, cursor: Cursor) -> Result<Url, Error> {
        let mut url = Url::parse(
            format!(
                "wss://{hostname}/subscribe",
                hostname = self.options.hostname
            )
            .as_ref(),
        )?;

        {
            let mut query_pairs = url.query_pairs_mut();

            for wanted_collection in &self.options.wanted_collections {
                query_pairs.append_pair("wantedCollections", wanted_collection.as_str());
            }
            for wanted_did in &self.options.wanted_dids {
                query_pairs.append_pair("wantedDids", wanted_did.as_str());
            }
            query_pairs.append_pair(
                "maxMessageSizeBytes",
                self.options.max_message_size_bytes.to_string().as_str(),
            );
            query_pairs.append_pair("cursor", cursor.get().to_string().as_str());
            query_pairs.append_pair(
                "compress",
                if self.options.compress {
                    "true"
                } else {
                    "false"
                },
            );
        }

        Ok(url)
    }

    pub async fn connect(&self, cursor: Cursor) -> Result<(), Error> {
        // TODO: retry scheme?
        let wss_url = self.wss_url(cursor)?;
        let (stream, _) = connect_async(wss_url.to_string())
            .await
            .map_err(Error::WssConnect)?;

        let (_, mut stream) = stream.split();

        loop {
            let message = stream
                .next()
                .await
                .ok_or(Error::ConnectionDropped)?
                .map_err(Error::JetstreamRead)?;
            self.message_tx.send(message).await?;
        }
    }
}
