use std::sync::Arc;
use std::sync::Mutex;
use std::io::Cursor;

use crate::store::{CommandRequest, CommandResponse, StateMachineStore};
use crate::network::Network;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = CommandRequest,
        R = CommandResponse,
);

pub type Raft = openraft::Raft<TypeConfig>;

pub mod typ {
    use openraft::BasicNode;

    use crate::app::NodeId;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;
}

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub state_machine_store: Arc<StateMachineStore>,
    pub network: Network,
    pub discovery: Mutex<Option<Arc<crate::discovery::MdnsDiscovery>>>,
}