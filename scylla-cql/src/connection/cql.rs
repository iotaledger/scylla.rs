// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::frame::{
    auth_challenge::AuthChallenge,
    auth_response::{AuthResponse, Authenticator},
    authenticate::Authenticate,
    consistency::Consistency,
    decoder::{Decoder, Frame},
    options::Options,
    query::Query,
    startup::Startup,
    supported::Supported,
};
use std::{
    convert::TryInto,
    io::{Error, ErrorKind},
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
};

use crate::compression::{MyCompression, UNCOMPRESSED};
use port_scanner::{local_port_available, request_open_port};
use std::collections::HashMap;

#[derive(Default)]
struct CqlBuilder<Auth: Authenticator> {
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
    tokens: Option<usize>, // todo
    dc: Option<String>,
    shard_id: u16,
    shard_aware_port: u16,
    shard_count: u16,
    msb: u8,
}

pub struct Token {
    token: i64,
    address: SocketAddr,
    dc: String,
    msb: u8,
    shard_count: u16,
}

impl<Auth: Authenticator> CqlBuilder<Auth> {
    fn new() -> Self {
        CqlBuilder::<Auth>::default()
    }
    fn address(mut self, address: SocketAddr) -> Self {
        self.address.replace(address);
        self
    }
    fn recv_buffer_size(mut self, recv_buffer_size: u32) -> Self {
        self.recv_buffer_size.replace(recv_buffer_size);
        self
    }
    fn send_buffer_size(mut self, send_buffer_size: u32) -> Self {
        self.send_buffer_size.replace(send_buffer_size);
        self
    }
    fn tokens(mut self) -> Self {
        self.tokens = true;
        self
    }
    fn set_local_addr(&mut self, local_addr: SocketAddr) {
        self.address.replace(local_addr);
    }
    async fn connect(&mut self) -> Result<(), Error> {
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
        let mut stream = socket.connect(self.address.unwrap()).await?;
        // create options frame
        let Options(opt_buf) = Options::new().build();
        // write_all options frame to stream
        stream.write_all(&opt_buf).await?;
        // collect_frame_response
        let buffer = collect_frame_response(&mut stream).await?;
        // Create Decoder from buffer. OPTIONS cannot be compressed as
        // the client and protocol didn't yet settle on compression algo (if any)
        let decoder = Decoder::new(buffer, UNCOMPRESSED);
        // make sure the frame response is not error
        if decoder.is_error() {
            // check if response is_error.
            return Err(Error::new(
                ErrorKind::Other,
                "CQL connection not supported due to CqlError",
            ));
        }
        assert!(decoder.is_supported());
        // decode supported options from decoder
        let supported = Supported::new(&decoder);
        // create empty hashmap options;
        let mut options: HashMap<String, String> = HashMap::new();
        // get the supported_cql_version option;
        let cql_version = supported.get_options().get("CQL_VERSION").unwrap().first().unwrap();
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
        let decoder = Decoder::new(buffer, MyCompression::get());
        if decoder.is_authenticate() {
            if self.authenticator.is_none() {
                let authenticate = Authenticate::new(&decoder);
                return Err(Error::new(
                    ErrorKind::Other,
                    "CQL connection not ready due to authenticator is not provided",
                ));
            }
            let auth_response = AuthResponse::new()
                .token(self.authenticator.as_ref().unwrap())
                .build(MyCompression::get());
            // write_all auth_response frame to stream;
            stream.write_all(&auth_response.0).await?;
            // collect_frame_response
            let buffer = collect_frame_response(&mut stream).await?;
            // Create Decoder from buffer.
            let decoder = Decoder::new(buffer, MyCompression::get());
            if decoder.is_error() {
                return Err(Error::new(ErrorKind::Other, "CQL connection not ready due to CqlError"));
            }
            if decoder.is_auth_challenge() {
                let auth_challenge = AuthChallenge::new(&decoder);
                return Err(Error::new(
                    ErrorKind::Other,
                    "CQL connection not ready due to Unsupported Auth Challenge",
                ));
            }
            assert!(decoder.is_auth_success());
        } else if decoder.is_error() {
            return Err(Error::new(ErrorKind::Other, "CQL connection not ready due to CqlError"));
        } else {
            assert!(decoder.is_ready());
        }
        // copy usefull options
        let shard: u16 = supported.get_options().get("SCYLLA_SHARD").unwrap()[0].parse().unwrap();
        let nr_shard: u16 = supported.get_options().get("SCYLLA_NR_SHARDS").unwrap()[0]
            .parse()
            .unwrap();
        let ignore_msb: u8 = supported.get_options().get("SCYLLA_SHARDING_IGNORE_MSB").unwrap()[0]
            .parse()
            .unwrap();
        let shard_aware_port: u16 = supported.get_options().get("SCYLLA_SHARD_AWARE_PORT").unwrap()[0]
            .parse()
            .unwrap();
        // create cqlconn
        let cqlconn = Cql {
            stream,
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

    async fn build(mut self) -> Result<Cql, Error> {
        // connect
        self.connect().await?;
        // take the cql_connection
        let cqlconn = self.cql.take().unwrap();
        // make sure to connect to the right shard(if provided)
        if let Some(requested_shard_id) = self.shard_id {
            if requested_shard_id != cqlconn.shard_id {
                if self.address.as_ref().unwrap().port() == cqlconn.shard_aware_port {
                    while let Some(requested_open_port) = request_open_port() {
                        let will_get_shard_id = requested_open_port % (cqlconn.shard_count as u16);
                        if will_get_shard_id != requested_shard_id {
                            let potential_open_port: u16 =
                                (requested_open_port - will_get_shard_id) + requested_shard_id;
                            // make sure the potential_open_port is open
                            if local_port_available(potential_open_port) {
                                let local_address =
                                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), potential_open_port);
                                self.set_local_addr(local_address);
                                // reconnect
                                self.connect().await?;
                                // take the cql_connection
                                let cqlconn = self.cql.take().unwrap();
                                // assert shard_id is equal
                                assert_eq!(cqlconn.shard_id, requested_shard_id);
                                // todo fetch tokens (if set)

                                return Ok(cqlconn);
                            } else {
                                // continue, request new open_port
                                continue;
                            }
                        }
                    }
                    // return error no fd/open_ports anymore?
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        "CQL connection not established due to lack of open ports",
                    ));
                } else {
                    // not shard_aware_port
                    // buffer connections temporary to force scylla connects us to new shard_id
                    let mut conns = Vec::new();
                    // loop till we connect to the right shard_id
                    loop {
                        match self.connect().await {
                            Ok(_) => {
                                let cqlconn = self.cql.take().unwrap();
                                if cqlconn.shard_id == requested_shard_id {
                                    // return
                                    // TODO fetch tokens (if set)
                                    return Ok(cqlconn);
                                } else if requested_shard_id >= cqlconn.shard_count {
                                    // error as it's impossible to connect to shard_id doesn't exist
                                    return Err(Error::new(ErrorKind::Other, "shard_id does not exist."));
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
                // todo fetch tokens (if set)
                return Ok(cqlconn);
            }
        } else {
            // shard_id not provided, so connection is ready
            // todo fetch tokens (if set)
            return Ok(cqlconn);
        }
    }
}

impl Cql {
    pub async fn fetch_tokens(&mut self) -> Result<(), Error> {
        todo!()
    }
}

async fn collect_frame_response(stream: &mut TcpStream) -> Result<Vec<u8>, Error> {
    // create buffer
    let mut buffer = vec![0; 9];
    // read response into buffer
    stream.read_exact(&mut buffer).await?;
    let body_length = i32::from_be_bytes(buffer[5..9].try_into().unwrap());
    // extend buffer
    buffer.resize((body_length + 9).try_into().unwrap(), 0);
    stream.read_exact(&mut buffer[9..]).await?;
    Ok(buffer)
}

// ----------- encoding scope -----------
/// Query the data center, broadcast address, and tokens from the ScyllaDB.
pub fn query() -> Vec<u8> {
    let Query(payload) = Query::new()
        .statement("SELECT data_center, broadcast_address, tokens FROM system.local")
        .consistency(Consistency::One)
        .build();
    payload
}
