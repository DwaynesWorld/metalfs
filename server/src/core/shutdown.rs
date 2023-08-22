use std::fmt;
use std::sync::RwLock;

use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq)]
/// Types of Shutdown states
pub enum ShutdownState {
    NotStarted,
    Started,
    Complete,
}

impl fmt::Display for ShutdownState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ShutdownState::NotStarted => write!(f, "NotStarted"),
            ShutdownState::Started => write!(f, "Started"),
            ShutdownState::Complete => write!(f, "Complete"),
        }
    }
}

#[derive(Debug)]
struct Inner {
    state: ShutdownState,
}

/// Listens for the shutdown signal.
///
/// Shutdown is signalled using a `Notify`. Only a single value is
/// ever sent. Once a value has been sent via the channel, the shutdown begins.
///
/// The `Shutdown` struct listens for the signal and tracks that the signal has
/// been received. Callers may query for whether the shutdown signal has been
/// received or not.
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// State of the shutdown signal.
    inner: RwLock<Inner>,

    /// The channel used to listen for shutdown begin.
    begin: Notify,

    /// The channel used to signal for shutdown completion.
    complete: Notify,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `Notify`.
    pub(crate) fn new() -> Shutdown {
        Shutdown {
            inner: RwLock::new(Inner {
                state: ShutdownState::NotStarted,
            }),
            begin: Notify::new(),
            complete: Notify::new(),
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub(crate) fn is_shutdown(&self) -> bool {
        let state = self.inner.read().unwrap().state;
        state.eq(&ShutdownState::Started) || state.eq(&ShutdownState::Complete)
    }

    /// Wait for the begin shutdown notice.
    pub(crate) async fn wait_begin(&self) {
        self.begin.notified().await
    }

    /// Begin the shutdown.
    pub(crate) fn begin(&self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.is_shutdown() {
            return;
        }

        self.begin.notify_waiters();

        // Remember that the signal has been received.
        let mut inner = self.inner.write().unwrap();
        inner.state = ShutdownState::Started;
    }

    /// Wait for the shutdown to complete.
    pub(crate) async fn wait_complete(&self) {
        self.complete.notified().await
    }

    /// Complete the shutdown.
    pub(crate) fn complete(&self) {
        self.complete.notify_waiters();

        // Remember that the signal has been received.
        let mut inner = self.inner.write().unwrap();
        inner.state = ShutdownState::Complete;
    }
}
