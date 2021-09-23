// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::tokens::{Info, Row};
use crate::cql::{
    compression::{MyCompression, UNCOMPRESSED},
    frame::{
        auth_challenge::AuthChallenge,
        auth_response::{AllowAllAuth, AuthResponse, Authenticator, PasswordAuth},
        authenticate::Authenticate,
        consistency::Consistency,
        decoder::{Decoder, Frame},
        options::Options,
        query::Query,
        rows::Rows,
        startup::Startup,
        supported::Supported,
        Statements,
    },
};
use anyhow::{anyhow, bail, ensure};
use port_scanner::{local_port_available, request_open_port};
use std::{
    collections::HashMap,
    convert::TryInto,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
};

#[derive(Default)]
/// CqlBuilder struct to establish cql connection with the provided configurations
pub struct CqlBuilder<Auth: Authenticator> {
    address: Option<SocketAddr>,
    local_addr: Option<SocketAddr>,
    tokens: bool,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    shard_id: Option<u16>,
    authenticator: Option<Auth>,
    cql: Option<Cql>,
}
/// CQL connection structure.
pub struct Cql {
    stream: TcpStream,
    address: SocketAddr,
    tokens: Option<Vec<i64>>,
    dc: Option<String>,
    shard_id: u16,
    shard_aware_port: u16,
    shard_count: u16,
    msb: u8,
}

impl<Auth: Authenticator> CqlBuilder<Auth> {
    /// Create CqlBuilder associated with Auth type;
    pub fn new() -> Self {
        CqlBuilder::<Auth>::default()
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
        let Options(opt_buf) = Options::new().build();
        // write_all options frame to stream
        stream.write_all(&opt_buf).await?;
        // collect_frame_response
        let buffer = collect_frame_response(&mut stream).await?;
        // Create Decoder from buffer. OPTIONS cannot be compressed as
        // the client and protocol didn't yet settle on compression algo (if any)
        let decoder = Decoder::new(buffer, UNCOMPRESSED)?;
        // make sure the frame response is not error
        if decoder.is_error()? {
            // check if response is_error.
            bail!("CQL connection not supported due to CqlError: {}", decoder.get_error()?);
        }
        ensure!(decoder.is_supported()?, "CQL connection not supported!");
        // decode supported options from decoder
        let supported = Supported::new(&decoder)?;
        // create empty hashmap options;
        let mut options: HashMap<String, String> = HashMap::new();
        // get the supported_cql_version option;
        let cql_version = supported
            .get_options()
            .get("CQL_VERSION")
            .ok_or_else(|| anyhow!("Cannot read supported CQL versions!"))?
            .first()
            .ok_or_else(|| anyhow!("Cannot read supported CQL version!"))?;
        // insert the supported_cql_version option into the options;
        options.insert("CQL_VERSION".to_owned(), cql_version.to_owned());
        // insert the supported_compression option into the options if it was set.;
        if let Some(compression) = MyCompression::option() {
            options.insert("COMPRESSION".to_owned(), compression.to_owned());
        }
        // create startup frame using the selected options;
        let Startup(startup_buf) = Startup::new().options(&options).build();
        // write_all startup frame to stream;
        stream.write_all(&startup_buf).await?;
        let buffer = collect_frame_response(&mut stream).await?;
        // Create Decoder from buffer.
        let decoder = Decoder::new(buffer, MyCompression::get())?;
        if decoder.is_authenticate()? {
            if self.authenticator.is_none() {
                Authenticate::new(&decoder)?;
                bail!("CQL connection not ready due to authenticator is not provided");
            }
            let auth_response = AuthResponse::new()
                .token(
                    self.authenticator
                        .as_ref()
                        .ok_or_else(|| anyhow!("Failed to read Auth Response!"))?,
                )
                .build(MyCompression::get())?;
            // write_all auth_response frame to stream;
            stream.write_all(&auth_response.0).await?;
            // collect_frame_response
            let buffer = collect_frame_response(&mut stream).await?;
            // Create Decoder from buffer.
            let decoder = Decoder::new(buffer, MyCompression::get())?;
            if decoder.is_error()? {
                bail!("CQL connection not ready due to CqlError: {}", decoder.get_error()?);
            }
            if decoder.is_auth_challenge()? {
                AuthChallenge::new(&decoder)?;
                bail!("CQL connection not ready due to Unsupported Auth Challenge");
            }
            ensure!(decoder.is_auth_success()?, "Authorization unsuccessful!");
        } else if decoder.is_error()? {
            bail!("CQL connection not ready due to CqlError: {}", decoder.get_error()?);
        } else {
            ensure!(decoder.is_ready()?, "Decoder is not ready!");
        }
        // copy usefull options
        let shard: u16 = supported
            .get_options()
            .get("SCYLLA_SHARD")
            .ok_or_else(|| anyhow!("Cannot read supported scylla shards!"))?
            .first()
            .ok_or_else(|| anyhow!("Cannot read scylla shard!"))?
            .parse()?;
        let nr_shard: u16 = supported
            .get_options()
            .get("SCYLLA_NR_SHARDS")
            .ok_or_else(|| anyhow!("Cannot read supported scylla NR shards!"))?
            .first()
            .ok_or_else(|| anyhow!("Cannot read scylla NR shard!"))?
            .parse()?;
        let ignore_msb: u8 = supported
            .get_options()
            .get("SCYLLA_SHARDING_IGNORE_MSB")
            .ok_or_else(|| anyhow!("Cannot read supported scylla ignore MSBs!"))?
            .first()
            .ok_or_else(|| anyhow!("Cannot read scylla scylla ignore MSB!"))?
            .parse()?;
        let shard_aware_port: u16 = supported
            .get_options()
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
        };
        self.cql.replace(cqlconn);
        Ok(())
    }
    /// Build the CqlBuilder and then try to connect
    pub async fn build(mut self) -> anyhow::Result<Cql> {
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
                                assert_eq!(cqlconn.shard_id, requested_shard_id);
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
                            assert_eq!(cqlconn.shard_id, requested_shard_id);
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

impl Into<TcpStream> for Cql {
    fn into(self) -> TcpStream {
        self.stream
    }
}

impl Cql {
    /// Create new cql connection builder struct
    pub fn new() -> CqlBuilder<AllowAllAuth> {
        CqlBuilder::<AllowAllAuth>::default()
    }
    /// Create new cql connection builder struct with attached authenticator
    pub fn with_auth(user: String, pass: String) -> CqlBuilder<PasswordAuth> {
        let mut cql_builder = CqlBuilder::<PasswordAuth>::default();
        let auth = PasswordAuth::new(user, pass);
        cql_builder.authenticator.replace(auth);
        cql_builder
    }
    async fn fetch_tokens(&mut self) -> anyhow::Result<()> {
        // create query to fetch tokens and info from system.local;
        let query = fetch_tokens_query()?;
        // write_all query to the stream
        self.stream.write_all(query.as_slice()).await?;
        // collect_frame_response
        let buffer = collect_frame_response(&mut self.stream).await?;
        // Create Decoder from buffer.
        let decoder = Decoder::new(buffer, MyCompression::get())?;

        if decoder.is_rows()? {
            let Row { data_center, tokens } = Info::new(decoder)?.next().ok_or(anyhow!("No info found!"))?;
            self.dc.replace(data_center);
            self.tokens.replace(
                tokens
                    .iter()
                    .map(|x| Ok(x.parse()?))
                    .filter_map(|r: anyhow::Result<i64>| r.ok())
                    .collect(),
            );
        } else {
            bail!("CQL connection didn't return rows due to CqlError");
        }
        Ok(())
    }
    /// Get the socket stream behind the cql connection
    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
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
fn fetch_tokens_query() -> anyhow::Result<Vec<u8>> {
    let Query(payload) = Query::new()
        .statement("SELECT data_center, tokens FROM system.local")
        .consistency(Consistency::One)
        .build()?;
    Ok(payload)
}
