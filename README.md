# atproto-jetstream

(ATProto Jetstream)[https://github.com/bluesky-social/jetstream] consumer tool written in Rust.

[The statusphere app](src/bin/statusphere.rs) demonstrates sample usage by displaying live `xyz.statusphere.status` status updates (see the [AT Protocol example app](https://github.com/bluesky-social/statusphere-example-app)), along with a playback of the past two hours.

This is a work in progress, and should only be considered a demonstration; at the very least, it lacks some standard robustness considerations (e.g., retrying connections, automatic reconnect when failing).