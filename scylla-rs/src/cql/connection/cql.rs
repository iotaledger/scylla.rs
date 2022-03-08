// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::cql::*;
use anyhow::{
    anyhow,
    bail,
};
use compression::Compression;
use port_scanner::{
    local_port_available,
    request_open_port,
};
use std::{
    collections::HashMap,
    convert::TryInto,
    marker::PhantomData,
    net::{
        IpAddr,
        Ipv4Addr,
        SocketAddr,
    },
};
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::{
        TcpSocket,
        TcpStream,
    },
};

/// CqlBuilder struct to establish cql connection with the provided configurations
pub struct CqlBuilder<Auth: Authenticator, C: Compression> {
    address: Option<SocketAddr>,
    local_addr: Option<SocketAddr>,
    tokens: bool,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    shard_id: Option<u16>,
    authenticator: Option<Auth>,
    cql: Option<Cql<C>>,
}

impl<Auth: Authenticator, C: Compression> core::fmt::Debug for CqlBuilder<Auth, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CqlBuilder")
            .field("address", &self.address)
            .field("local_addr", &self.local_addr)
            .field("tokens", &self.tokens)
            .field("recv_buffer_size", &self.recv_buffer_size)
            .field("send_buffer_size", &self.send_buffer_size)
            .field("shard_id", &self.shard_id)
            .field("cql", &self.cql)
            .finish()
    }
}

impl<Auth: Authenticator, C: Compression> Default for CqlBuilder<Auth, C> {
    fn default() -> Self {
        Self {
            address: Default::default(),
            local_addr: Default::default(),
            tokens: Default::default(),
            recv_buffer_size: Default::default(),
            send_buffer_size: Default::default(),
            shard_id: Default::default(),
            authenticator: Default::default(),
            cql: Default::default(),
        }
    }
}

/// CQL connection structure.
pub struct Cql<C: Compression> {
    stream: TcpStream,
    address: SocketAddr,
    tokens: Option<Vec<i64>>,
    dc: Option<String>,
    shard_id: u16,
    shard_aware_port: u16,
    shard_count: u16,
    msb: u8,
    compression: PhantomData<fn(C) -> C>,
}

impl<C: Compression> core::fmt::Debug for Cql<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cql")
            .field("stream", &self.stream)
            .field("address", &self.address)
            .field("tokens", &self.tokens)
            .field("dc", &self.dc)
            .field("shard_id", &self.shard_id)
            .field("shard_aware_port", &self.shard_aware_port)
            .field("shard_count", &self.shard_count)
            .field("msb", &self.msb)
            .field("compression", &std::any::type_name::<C>())
            .finish()
    }
}

impl<Auth: Authenticator, C: Compression> CqlBuilder<Auth, C> {
    /// Create CqlBuilder associated with Auth type;
    pub fn new() -> Self {
        CqlBuilder::default()
    }
    /// Add scylla broadcast_address
    pub fn address(mut self, address: SocketAddr) -> Self {
        self.address.replace(address);
        self
    }
    /// Add an optional recv_buffer_size
    pub fn recv_buffer_size(mut self, recv_buffer_size: Option<u32>) -> Self {
        self.recv_buffer_size = recv_buffer_size;
        self
    }
    /// Add an optional send_buffer_size
    pub fn send_buffer_size(mut self, send_buffer_size: Option<u32>) -> Self {
        self.send_buffer_size = send_buffer_size;
        self
    }
    /// Instruct the builder to fetch cql tokens from the connection once established
    pub fn tokens(mut self) -> Self {
        self.tokens = true;
        self
    }
    /// Instruct the builder to connect to scylla shard with shard_id
    pub fn shard_id(mut self, shard_id: u16) -> Self {
        self.shard_id.replace(shard_id);
        self
    }
    /// Instruct the builder to use the provided authenticator for establishing the connection
    pub fn authenticator(mut self, auth: Auth) -> Self {
        self.authenticator.replace(auth);
        self
    }
    fn set_local_addr(&mut self, local_addr: SocketAddr) {
        self.local_addr.replace(local_addr);
    }
    async fn connect(&mut self) -> anyhow::Result<()> {
        let socket = TcpSocket::new_v4()?;
        if let Some(local_addr) = self.local_addr {
            // set client side port
            socket.bind(local_addr)?;
        }
        // set socket flags
        if let Some(recv_buffer_size) = self.recv_buffer_size {
            socket.set_recv_buffer_size(recv_buffer_size)?
        }
        if let Some(send_buffer_size) = self.send_buffer_size {
            socket.set_send_buffer_size(send_buffer_size)?
        }
        let mut stream = socket
            .connect(self.address.ok_or_else(|| anyhow!("Address does not exist!"))?)
            .await?;
        // create options frame
        let opts_frame = RequestFrame::from(OptionsFrame);
        // write_all options frame to stream
        stream.write_all(&opts_frame.build_payload()).await?;
        // collect_frame_response
        let buffer = collect_frame_response(&mut stream).await?;
        // Create Decoder from buffer. OPTIONS cannot be compressed as
        // the client and protocol didn't yet settle on compression algo (if any)
        let response_frame = ResponseFrame::decode::<C>(buffer)?;
        // make sure the frame response is not error
        let supported = match response_frame.body() {
            ResponseBody::Supported(sup_frame) => {
                // create empty hashmap options;
                let mut options: HashMap<String, String> = HashMap::new();
                // get the supported_cql_version option;
                let cql_version = sup_frame
                    .options()
                    .get("CQL_VERSION")
                    .ok_or_else(|| anyhow!("Cannot read supported CQL versions!"))?
                    .first()
                    .ok_or_else(|| anyhow!("Cannot read supported CQL version!"))?;
                // insert the supported_cql_version option into the options;
                options.insert("CQL_VERSION".to_owned(), cql_version.to_owned());
                // insert the supported_compression option into the options if it was set.;
                if let Some(compression) = C::KIND {
                    options.insert("COMPRESSION".to_owned(), compression.to_string());
                }
                // create startup frame using the selected options;
                let startup_frame = RequestFrame::from(StartupFrameBuilder::default().options(options).build()?);
                // write_all startup frame to stream;
                stream.write_all(&startup_frame.build_payload()).await?;
                let buffer = collect_frame_response(&mut stream).await?;
                // Create Decoder from buffer.
                let response_frame = ResponseFrame::decode::<C>(buffer)?;
                match response_frame.body() {
                    ResponseBody::Authenticate(_auth_frame) => {
                        if self.authenticator.is_none() {
                            bail!("CQL connection not ready due to authenticator is not provided");
                        }
                        let auth_response_frame = RequestFrame::from(
                            AuthResponseFrameBuilder::default()
                                .auth_token(
                                    self.authenticator
                                        .as_ref()
                                        .ok_or_else(|| anyhow!("Failed to read Auth Response!"))?,
                                )
                                .build()?,
                        );
                        // write_all auth_response frame to stream;
                        stream.write_all(&auth_response_frame.build_payload()).await?;
                        // collect_frame_response
                        let buffer = collect_frame_response(&mut stream).await?;
                        // Create Decoder from buffer.
                        let response_frame = ResponseFrame::decode::<C>(buffer)?;
                        match response_frame.body() {
                            ResponseBody::AuthSuccess(_) => (),
                            ResponseBody::AuthChallenge(_auth_chal) => {
                                todo!("Support auth challenges")
                            }
                            ResponseBody::Error(err) => {
                                bail!("CQL connection not ready due to CqlError: {}", err);
                            }
                            _ => bail!("Invalid response from server!"),
                        }
                    }
                    ResponseBody::Ready(_) => (),
                    ResponseBody::Error(err) => {
                        bail!("CQL connection not ready due to CqlError: {}", err);
                    }
                    _ => bail!("Invalid response from server!"),
                }
                sup_frame.options()
            }
            ResponseBody::Error(err) => {
                bail!("CQL connection not supported due to CqlError: {}", err);
            }
            _ => {
                return Err(anyhow!("Unexpected response from server!"));
            }
        };
        // copy useful options
        let shard: u16 = supported
            .get("SCYLLA_SHARD")
            .ok_or_else(|| anyhow!("Cannot read supported scylla shards!"))?
            .first()
            .ok_or_else(|| anyhow!("Cannot read scylla shard!"))?
            .parse()?;
        let nr_shard: u16 = supported
            .get("SCYLLA_NR_SHARDS")
            .ok_or_else(|| anyhow!("Cannot read supported scylla NR shards!"))?
            .first()
            .ok_or_else(|| anyhow!("Cannot read scylla NR shard!"))?
            .parse()?;
        let ignore_msb: u8 = supported
            .get("SCYLLA_SHARDING_IGNORE_MSB")
            .ok_or_else(|| anyhow!("Cannot read supported scylla ignore MSBs!"))?
            .first()
            .ok_or_else(|| anyhow!("Cannot read scylla scylla ignore MSB!"))?
            .parse()?;
        let shard_aware_port: u16 = supported
            .get("SCYLLA_SHARD_AWARE_PORT")
            .ok_or_else(|| {
                anyhow!("Cannot read supported scylla shard aware ports! Try upgrading your Scylla to latest release!")
            })?
            .first()
            .ok_or_else(|| {
                anyhow!("Cannot read supported scylla shard aware port! Try upgrading your Scylla to latest release!")
            })?
            .parse()?;
        // create cqlconn
        let cqlconn = Cql {
            stream,
            address: self.address.ok_or_else(|| anyhow!("Address does not exist!"))?,
            tokens: None,
            shard_id: shard,
            shard_aware_port,
            shard_count: nr_shard,
            msb: ignore_msb,
            dc: None,
            compression: PhantomData,
        };
        self.cql.replace(cqlconn);
        Ok(())
    }
    /// Build the CqlBuilder and then try to connect
    pub async fn build(mut self) -> anyhow::Result<Cql<C>> {
        // connect
        self.connect().await?;
        // take the cql_connection
        let mut cqlconn = self.cql.take().ok_or_else(|| anyhow!("No CQL connection!"))?;
        // make sure to connect to the right shard(if provided)
        if let Some(requested_shard_id) = self.shard_id {
            if requested_shard_id != cqlconn.shard_id {
                if self
                    .address
                    .as_ref()
                    .ok_or_else(|| anyhow!("Address does not exist!"))?
                    .port()
                    == cqlconn.shard_aware_port
                {
                    while let Some(requested_open_port) = request_open_port() {
                        let will_get_shard_id = requested_open_port % (cqlconn.shard_count as u16);
                        if will_get_shard_id != requested_shard_id {
                            let potential_open_port: u16 =
                                (requested_open_port - will_get_shard_id) + requested_shard_id;
                            // make sure the potential_open_port is open
                            if local_port_available(potential_open_port) {
                                let local_address =
                                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), potential_open_port);
                                self.set_local_addr(local_address);
                                // reconnect
                                self.connect().await?;
                                // take the cql_connection
                                let mut cqlconn = self.cql.take().ok_or_else(|| anyhow!("No CQL connection!"))?;
                                // assert shard_id is equal
                                if self.tokens {
                                    cqlconn.fetch_tokens().await?;
                                }
                                return Ok(cqlconn);
                            } else {
                                // continue, request new open_port
                                continue;
                            }
                        } else {
                            let local_address =
                                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), requested_open_port);
                            self.set_local_addr(local_address);
                            // reconnect
                            self.connect().await?;
                            // take the cql_connection
                            let mut cqlconn = self.cql.take().ok_or_else(|| anyhow!("No CQL connection!"))?;
                            // assert shard_id is equal
                            if self.tokens {
                                cqlconn.fetch_tokens().await?;
                            }
                            return Ok(cqlconn);
                        }
                    }
                    // return error no fd/open_ports anymore?
                    bail!("CQL connection not established due to lack of open ports");
                } else {
                    // not shard_aware_port
                    // buffer connections temporary to force scylla connects us to new shard_id
                    let mut conns = Vec::new();
                    // loop till we connect to the right shard_id
                    loop {
                        match self.connect().await {
                            Ok(_) => {
                                let mut cqlconn = self.cql.take().ok_or_else(|| anyhow!("No CQL connection!"))?;
                                if cqlconn.shard_id == requested_shard_id {
                                    if self.tokens {
                                        cqlconn.fetch_tokens().await?;
                                    }
                                    return Ok(cqlconn);
                                } else if requested_shard_id >= cqlconn.shard_count {
                                    // error as it's impossible to connect to shard_id doesn't exist
                                    bail!("Requested shard ID does not exist: {}", requested_shard_id);
                                } else {
                                    if conns.len() > cqlconn.shard_count as usize {
                                        // clear conns otherwise we are going to overflow the memory
                                        conns.clear();
                                    }
                                    conns.push(cqlconn);
                                    // continue to retry
                                    continue;
                                }
                            }
                            Err(err) => {
                                return Err(err);
                            }
                        }
                    }
                }
            } else {
                // FOUND connection
                if self.tokens {
                    cqlconn.fetch_tokens().await?;
                }
                return Ok(cqlconn);
            }
        } else {
            // shard_id not provided, so connection is ready
            if self.tokens {
                cqlconn.fetch_tokens().await?;
            }
            return Ok(cqlconn);
        }
    }
}

impl<C: Compression> Into<TcpStream> for Cql<C> {
    fn into(self) -> TcpStream {
        self.stream
    }
}

impl<C: Compression> Cql<C> {
    /// Create new cql connection builder struct
    pub fn new() -> CqlBuilder<AllowAllAuth, C> {
        Default::default()
    }
    /// Create new cql connection builder struct with attached authenticator
    pub fn with_auth(user: String, pass: String) -> CqlBuilder<PasswordAuth, C> {
        let mut cql_builder = CqlBuilder::<PasswordAuth, C>::default();
        let auth = PasswordAuth::new(user, pass);
        cql_builder.authenticator.replace(auth);
        cql_builder
    }
    async fn fetch_tokens(&mut self) -> anyhow::Result<()> {
        // create query to fetch tokens and info from system.local;
        let query = fetch_tokens_query::<C>()?;
        // write_all query to the stream
        self.stream.write_all(&query).await?;
        // collect_frame_response
        let buffer = collect_frame_response(&mut self.stream).await?;
        // Create Decoder from buffer.
        let frame = ResponseFrame::decode::<C>(buffer)?;
        match frame.body() {
            ResponseBody::Result(res) => match res.kind() {
                ResultBodyKind::Rows(r) => {
                    let (data_center, tokens) = r
                        .iter::<(String, Vec<String>)>()
                        .next()
                        .ok_or(anyhow!("No info found!"))?;
                    self.dc.replace(data_center);
                    self.tokens.replace(
                        tokens
                            .iter()
                            .map(|x| Ok(x.parse()?))
                            .filter_map(|r: anyhow::Result<i64>| r.ok())
                            .collect(),
                    );
                }
                _ => bail!("Unexpected result kind"),
            },
            ResponseBody::Error(err) => {
                bail!("CQL connection didn't return rows due to CqlError: {}", err);
            }
            _ => bail!("Unexpected body kind"),
        }
        Ok(())
    }
    /// Get the socket stream behind the cql connection
    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }
    /// Split the cql connection into Owned read and write halfs
    pub fn split(self) -> (tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf) {
        let stream: TcpStream = self.into();
        stream.into_split()
    }
    /// Take the associated tokens of the connected scylla node
    pub fn take_tokens(&mut self) -> Option<Vec<i64>> {
        self.tokens.take()
    }
    /// Take DataCenter of the connected scylla node
    pub fn take_dc(&mut self) -> Option<String> {
        self.dc.take()
    }
    /// Get the shard_id of the connection
    pub fn shard_id(&self) -> u16 {
        self.shard_id
    }
    /// Get the shard_count of the connection
    pub fn shard_count(&self) -> u16 {
        self.shard_count
    }
    /// Get the address of the connection
    pub fn address(&self) -> SocketAddr {
        self.address.clone()
    }
    /// Get the most significant bit (msb)
    pub fn msb(&self) -> u8 {
        self.msb
    }
}

async fn collect_frame_response(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    // create buffer
    let mut buffer = vec![0; 9];
    // read response into buffer
    stream.read_exact(&mut buffer).await?;
    let body_length = i32::from_be_bytes(buffer[5..9].try_into()?);
    // extend buffer
    buffer.resize((body_length + 9).try_into()?, 0);
    stream.read_exact(&mut buffer[9..]).await?;
    Ok(buffer)
}

/// Query the data center, and tokens from the ScyllaDB.
fn fetch_tokens_query<C: Compression>() -> anyhow::Result<Vec<u8>> {
    let frame = RequestFrame::from(
        QueryFrameBuilder::default()
            .statement("SELECT data_center, tokens FROM system.local".to_owned())
            .consistency(Consistency::One)
            .build()?,
    );
    let payload = C::compress(frame.build_payload())?;
    Ok(payload)
}
