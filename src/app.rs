use std::sync::Arc;
use std::io::Cursor;

use crate::store::{LogStore, Request, Response, StateMachineStore};

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
);

pub type Raft = openraft::Raft<TypeConfig>;

pub mod typ {
    use openraft::BasicNode;

    use crate::app::NodeId;
    use crate::app::TypeConfig;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub log_store: LogStore<TypeConfig>,
    pub state_machine_store: Arc<StateMachineStore>,
    pub config: Arc<openraft::Config>,
}