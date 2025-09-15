use anyhow::Result;
use serde::{Serialize, Deserialize};
use qlib_rs::{StoreMessage, Snapshot};
use rkyv::Deserialize as RkyvDeserialize;

/// Magic bytes to identify protocol messages (4 bytes)
const PROTOCOL_MAGIC: [u8; 4] = [0x51, 0x43, 0x4F, 0x52]; // "QCOR" in ASCII

/// Maximum message size to prevent memory exhaustion (16MB)
const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

/// Binary protocol header (12 bytes total)
#[derive(Debug, Clone)]
pub struct MessageHeader {
    /// Magic bytes for protocol identification
    pub magic: [u8; 4],
    /// Message length (excluding header)
    pub length: u32,
    /// Message type identifier
    pub message_type: u32,
}

impl MessageHeader {
    const SIZE: usize = 12;
    
    pub fn new(message_type: u32, length: u32) -> Self {
        Self {
            magic: PROTOCOL_MAGIC,
            length,
            message_type,
        }
    }
    
    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.length.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.message_type.to_le_bytes());
        bytes
    }
    
    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Result<Self> {
        let magic = [bytes[0], bytes[1], bytes[2], bytes[3]];
        if magic != PROTOCOL_MAGIC {
            return Err(anyhow::anyhow!("Invalid protocol magic"));
        }
        
        let length = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if length > MAX_MESSAGE_SIZE {
            return Err(anyhow::anyhow!("Message too large: {} bytes", length));
        }
        
        let message_type = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        
        Ok(Self {
            magic,
            length,
            message_type,
        })
    }
}

/// Message types for the binary protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    // Core store messages (1000-1999)
    StoreMessage = 1000,        // Uses bincode for existing StoreMessage compatibility
    FastStoreMessage = 1001,    // Uses rkyv for high-performance operations
    
    // Peer messages (2000-2999)
    PeerStartup = 2000,
    PeerFullSyncRequest = 2001,
    PeerFullSyncResponse = 2002,
    PeerSyncRequest = 2003,
    
    // Response messages (9000-9999)
    Response = 9000,
    Error = 9999,
}

impl MessageType {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1000 => Some(Self::StoreMessage),
            1001 => Some(Self::FastStoreMessage),
            2000 => Some(Self::PeerStartup),
            2001 => Some(Self::PeerFullSyncRequest),
            2002 => Some(Self::PeerFullSyncResponse),
            2003 => Some(Self::PeerSyncRequest),
            9000 => Some(Self::Response),
            9999 => Some(Self::Error),
            _ => None,
        }
    }
    
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Fast Store operations that can be serialized with rkyv for zero-copy
#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[archive(check_bytes)]
pub struct FastStoreRequest {
    pub id: String,
    pub operation: FastOperation,
}

#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[archive(check_bytes)]
pub enum FastOperation {
    /// Fast entity existence check - just needs entity ID bytes
    EntityExists { entity_id_bytes: Vec<u8> },
    /// Fast field existence check 
    FieldExists { entity_type: String, field_type: String },
    /// Fast notification (pre-serialized)
    Notification { data: Vec<u8> },
    /// Fast write requests (pre-serialized)
    WriteRequests { data: Vec<u8> },
}

#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[archive(check_bytes)]
pub struct FastStoreResponse {
    pub id: String,
    pub result: FastResult,
}

#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[archive(check_bytes)]
pub enum FastResult {
    /// Boolean result for existence checks
    Bool(bool),
    /// Success indicator for write operations
    Success,
    /// Error with message
    Error(String),
    /// Raw data response (pre-serialized)
    Data(Vec<u8>),
}

/// Peer startup message for rkyv serialization
#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[archive(check_bytes)]
pub struct PeerStartup {
    pub machine_id: String,
    pub startup_time: u64,
}

/// Peer sync request for rkyv serialization  
#[derive(Debug, Clone)]
#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[archive(check_bytes)]
pub struct PeerSyncRequest {
    pub requests_data: Vec<u8>, // Pre-serialized requests
}

/// Protocol message wrapper
#[derive(Debug)]
pub enum ProtocolMessage {
    Store(StoreMessage),                        // Uses bincode (compatibility)
    FastStore(FastStoreRequest),                // Uses rkyv (performance)
    FastStoreResponse(FastStoreResponse),       // Uses rkyv (performance)
    PeerStartup(PeerStartup),                   // Uses rkyv (performance)
    PeerFullSyncRequest { machine_id: String }, // Simple - uses bincode
    PeerFullSyncResponse { snapshot: Snapshot }, // Uses bincode (large data)
    PeerSyncRequest(PeerSyncRequest),           // Uses rkyv (performance)
    Response { id: String, data: Vec<u8> },     // Raw response data
    Error { id: Option<String>, message: String },
}

impl ProtocolMessage {
    /// Get the message type
    pub fn message_type(&self) -> MessageType {
        match self {
            Self::Store(_) => MessageType::StoreMessage,
            Self::FastStore(_) | Self::FastStoreResponse(_) => MessageType::FastStoreMessage,
            Self::PeerStartup(_) => MessageType::PeerStartup,
            Self::PeerFullSyncRequest { .. } => MessageType::PeerFullSyncRequest,
            Self::PeerFullSyncResponse { .. } => MessageType::PeerFullSyncResponse,
            Self::PeerSyncRequest(_) => MessageType::PeerSyncRequest,
            Self::Response { .. } => MessageType::Response,
            Self::Error { .. } => MessageType::Error,
        }
    }
    
    /// Serialize the message payload to bytes using the most appropriate method
    pub fn serialize_payload(&self) -> Result<Vec<u8>> {
        match self {
            // Use bincode for legacy StoreMessage compatibility
            Self::Store(msg) => bincode::serialize(msg)
                .map_err(|e| anyhow::anyhow!("Failed to serialize store message: {}", e)),
                
            // Use rkyv for high-performance operations
            Self::FastStore(req) => {
                rkyv::to_bytes::<_, 256>(req)
                    .map(|bytes| bytes.into_vec())
                    .map_err(|e| anyhow::anyhow!("Failed to serialize fast store request: {}", e))
            },
            Self::FastStoreResponse(resp) => {
                rkyv::to_bytes::<_, 256>(resp)
                    .map(|bytes| bytes.into_vec())
                    .map_err(|e| anyhow::anyhow!("Failed to serialize fast store response: {}", e))
            },
            
            // Use rkyv for peer messages (high frequency)
            Self::PeerStartup(startup) => {
                rkyv::to_bytes::<_, 256>(startup)
                    .map(|bytes| bytes.into_vec())
                    .map_err(|e| anyhow::anyhow!("Failed to serialize peer startup: {}", e))
            },
            Self::PeerSyncRequest(sync_req) => {
                rkyv::to_bytes::<_, 256>(sync_req)
                    .map(|bytes| bytes.into_vec())
                    .map_err(|e| anyhow::anyhow!("Failed to serialize peer sync request: {}", e))
            },
            
            // Use bincode for larger/less frequent messages
            Self::PeerFullSyncRequest { machine_id } => {
                #[derive(Serialize)]
                struct PeerFullSyncRequestPayload {
                    machine_id: String,
                }
                bincode::serialize(&PeerFullSyncRequestPayload {
                    machine_id: machine_id.clone(),
                }).map_err(|e| anyhow::anyhow!("Failed to serialize full sync request: {}", e))
            },
            Self::PeerFullSyncResponse { snapshot } => bincode::serialize(snapshot)
                .map_err(|e| anyhow::anyhow!("Failed to serialize snapshot: {}", e)),
                
            Self::Response { id, data } => {
                #[derive(Serialize)]
                struct ResponsePayload {
                    id: String,
                    data: Vec<u8>,
                }
                bincode::serialize(&ResponsePayload {
                    id: id.clone(),
                    data: data.clone(),
                }).map_err(|e| anyhow::anyhow!("Failed to serialize response: {}", e))
            },
            Self::Error { id, message } => {
                #[derive(Serialize)]
                struct ErrorPayload {
                    id: Option<String>,
                    message: String,
                }
                bincode::serialize(&ErrorPayload {
                    id: id.clone(),
                    message: message.clone(),
                }).map_err(|e| anyhow::anyhow!("Failed to serialize error: {}", e))
            },
        }
    }
    
    /// Deserialize the message payload from bytes using the appropriate method
    pub fn deserialize_payload(message_type: MessageType, payload: &[u8]) -> Result<Self> {
        match message_type {
            MessageType::StoreMessage => {
                let msg: StoreMessage = bincode::deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize store message: {}", e))?;
                Ok(Self::Store(msg))
            },
            MessageType::FastStoreMessage => {
                // Try to deserialize as request first, then response
                if let Ok(archived) = rkyv::check_archived_root::<FastStoreRequest>(payload) {
                    let req: FastStoreRequest = RkyvDeserialize::deserialize(archived, &mut rkyv::Infallible)
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize fast store request: {:?}", e))?;
                    Ok(Self::FastStore(req))
                } else if let Ok(archived) = rkyv::check_archived_root::<FastStoreResponse>(payload) {
                    let resp: FastStoreResponse = RkyvDeserialize::deserialize(archived, &mut rkyv::Infallible)
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize fast store response: {:?}", e))?;
                    Ok(Self::FastStoreResponse(resp))
                } else {
                    Err(anyhow::anyhow!("Failed to deserialize fast store message"))
                }
            },
            MessageType::PeerStartup => {
                let archived = rkyv::check_archived_root::<PeerStartup>(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to check archived peer startup: {}", e))?;
                let startup: PeerStartup = RkyvDeserialize::deserialize(archived, &mut rkyv::Infallible)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize peer startup: {:?}", e))?;
                Ok(Self::PeerStartup(startup))
            },
            MessageType::PeerFullSyncRequest => {
                #[derive(Deserialize)]
                struct PeerFullSyncRequestPayload {
                    machine_id: String,
                }
                let payload_data: PeerFullSyncRequestPayload = bincode::deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize full sync request: {}", e))?;
                Ok(Self::PeerFullSyncRequest {
                    machine_id: payload_data.machine_id,
                })
            },
            MessageType::PeerFullSyncResponse => {
                let snapshot: Snapshot = bincode::deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize snapshot: {}", e))?;
                Ok(Self::PeerFullSyncResponse { snapshot })
            },
            MessageType::PeerSyncRequest => {
                let archived = rkyv::check_archived_root::<PeerSyncRequest>(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to check archived peer sync request: {}", e))?;
                let sync_req: PeerSyncRequest = RkyvDeserialize::deserialize(archived, &mut rkyv::Infallible)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize peer sync request: {:?}", e))?;
                Ok(Self::PeerSyncRequest(sync_req))
            },
            MessageType::Response => {
                #[derive(Deserialize)]
                struct ResponsePayload {
                    id: String,
                    data: Vec<u8>,
                }
                let payload_data: ResponsePayload = bincode::deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize response: {}", e))?;
                Ok(Self::Response {
                    id: payload_data.id,
                    data: payload_data.data,
                })
            },
            MessageType::Error => {
                #[derive(Deserialize)]
                struct ErrorPayload {
                    id: Option<String>,
                    message: String,
                }
                let payload_data: ErrorPayload = bincode::deserialize(payload)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize error: {}", e))?;
                Ok(Self::Error {
                    id: payload_data.id,
                    message: payload_data.message,
                })
            },
        }
    }
}

/// Protocol encoder/decoder for handling message framing over TCP
pub struct ProtocolCodec;

impl ProtocolCodec {
    /// Encode a message into bytes ready for TCP transmission
    pub fn encode(message: &ProtocolMessage) -> Result<Vec<u8>> {
        let payload = message.serialize_payload()?;
        let header = MessageHeader::new(message.message_type().as_u32(), payload.len() as u32);
        
        let mut encoded = Vec::with_capacity(MessageHeader::SIZE + payload.len());
        encoded.extend_from_slice(&header.to_bytes());
        encoded.extend_from_slice(&payload);
        
        Ok(encoded)
    }
    
    /// Decode a message from a TCP stream
    /// Returns (message, bytes_consumed) or None if not enough data
    pub fn decode(buffer: &[u8]) -> Result<Option<(ProtocolMessage, usize)>> {
        // Need at least header size
        if buffer.len() < MessageHeader::SIZE {
            return Ok(None);
        }
        
        // Parse header
        let header_bytes: [u8; MessageHeader::SIZE] = buffer[0..MessageHeader::SIZE].try_into()
            .map_err(|_| anyhow::anyhow!("Failed to read header bytes"))?;
        let header = MessageHeader::from_bytes(&header_bytes)?;
        
        // Check if we have the full message
        let total_size = MessageHeader::SIZE + header.length as usize;
        if buffer.len() < total_size {
            return Ok(None);
        }
        
        // Parse message type
        let message_type = MessageType::from_u32(header.message_type)
            .ok_or_else(|| anyhow::anyhow!("Unknown message type: {}", header.message_type))?;
        
        // Deserialize payload
        let payload = &buffer[MessageHeader::SIZE..total_size];
        let message = ProtocolMessage::deserialize_payload(message_type, payload)?;
        
        Ok(Some((message, total_size)))
    }
}

/// TCP message buffer for handling partial reads
#[derive(Debug)]
pub struct MessageBuffer {
    buffer: Vec<u8>,
    capacity: usize,
}

impl MessageBuffer {
    pub fn new() -> Self {
        Self::with_capacity(64 * 1024) // 64KB initial capacity
    }
    
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            capacity,
        }
    }
    
    /// Add data to the buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
        
        // Prevent buffer from growing indefinitely
        if self.buffer.capacity() > self.capacity * 4 {
            self.buffer.shrink_to(self.capacity);
        }
    }
    
    /// Try to decode a message from the buffer
    /// Returns (message, remaining_buffer) if successful
    pub fn try_decode(&mut self) -> Result<Option<ProtocolMessage>> {
        match ProtocolCodec::decode(&self.buffer)? {
            Some((message, bytes_consumed)) => {
                // Remove consumed bytes from buffer
                self.buffer.drain(0..bytes_consumed);
                Ok(Some(message))
            },
            None => Ok(None),
        }
    }
}

impl Default for MessageBuffer {
    fn default() -> Self {
        Self::new()
    }
}