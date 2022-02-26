extern crate args;

use getopts::Occur;
use std::collections::HashMap;
use std::env;
use std::net::IpAddr;
use std::sync::Arc;
use args::{Args, ArgsError};
use async_stream::stream;
use cdrs_tokio::load_balancing::LoadBalancingStrategy;
use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{ConnectionManager, NodeTcpConfigBuilder};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::CdrsTransport;

use cdrs_tokio::types::from_cdrs::FromCdrsByName;
use cdrs_tokio::types::prelude::*;
use cdrs_tokio_helpers_derive::*;
use cdrs_tokio::frame::{Frame, Serialize};
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::query_values;
use futures_core::stream::Stream;
use futures::StreamExt;
use itertools::Itertools;

#[derive(Clone, Debug, TryFromRow, PartialEq)]
struct RowStruct {
    key: String,
    bootstrapped: String,
    broadcast_address: IpAddr,
    broadcast_port: i32,
    cluster_name: String
}

const PROGRAM_DESC: &'static str = "Transmitter migration for Cassandra";
const PROGRAM_NAME: &'static str = "transmitter-migration";

#[derive(Debug)]
struct Params {
    host: String,
    username: String,
    password: String
}

fn parse(input: &Vec<String>) -> core::result::Result<Option<Params>, ArgsError> {
    let mut args = Args::new(PROGRAM_NAME, PROGRAM_DESC);
    args.flag("h", "help", "Print the usage menu");
    args.option("H",
                "host",
                "host for cassandra",
                "localhost:9042",
                Occur::Req,
                None);
    args.option("u",
                "username",
                "username for cassandra",
                "cassandra",
                Occur::Req,
                None);
    args.option("p",
                "password",
                "password for cassandra",
                "******",
                Occur::Req,
                Some(String::from("output.log")));

    args.parse(input)?;

    let help = args.value_of("help")?;
    if help {
        args.full_usage();
        return Ok(None);
    }

    let host = args.value_of::<String>("host").expect("host must provide");
    let username = args.value_of::<String>("username").expect("username must provide");
    let password = args.value_of::<String>("password").expect("password must provide");

    Ok(Some(Params { host, username ,password }))
}

#[tokio::main]
async fn main() {
    let args = env::args().collect_vec();
    match parse(&args) {
        Ok(Some(param)) => run(param.host, param.username, param.password).await,
        Ok(None) => (),
        Err(e) => panic!("args error, {:?}", e)
    }
}

async fn run(host: String, username: String, password: String) {

    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(host.into())
        .with_authenticator_provider(Arc::new(StaticPasswordAuthenticatorProvider::new(username, password)))
        .build()
        .await
        .unwrap();
    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config).build();

    let entity_stream = query_entity(&session);
    tokio::pin!(entity_stream);

    while let Some(e) = entity_stream.next().await {
        println!("{:?}", e);
    }

    // let row = EntityChange {
    //     entity_type: String::from("ApplicantStandardResume"),
    //     tenant_id: String::from("boe-test"),
    //     update_time_bucket: 1642540000000i64,
    //     update_time: 1642545521123i64,
    //     open_id: String::from("abc123")
    // };
    // insert_entity_change(&session, row).await;
    //
}

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct Entity {
    entity_type: String,
    tenant_id: String,
    open_id: String,
    create_time: i64,
    update_time: i64,
    stages: HashMap<String, Stage>
}

#[derive(Debug, Clone, PartialEq, IntoCdrsValue, TryFromUdt)]
struct Stage {
    meta: StageMeta,
    r#ref: String
}

#[derive(Debug, Clone, PartialEq, IntoCdrsValue, TryFromUdt)]
struct StageMeta {
    mime_type: String,
    source: String,
    editor: String,
    create_time: i64,
    update_time: i64,
    version: i32,
}

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct EntityChange {
    entity_type: String,
    tenant_id: String,
    update_time_bucket: i64,
    update_time: i64,
    open_id: String,
}

impl EntityChange {
    fn into_query_values(self) -> QueryValues {
        query_values!(
            "entity_type" => self.entity_type,
            "tenant_id" => self.tenant_id,
            "update_time_bucket" => self.update_time_bucket,
            "update_time" => self.update_time,
            "open_id" => self.open_id
        )
    }
}

fn query_entity<
    T: CdrsTransport,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static
>(session: &Session<T, CM, LB>) -> impl Stream<Item = Entity> + '_ {
    stream! {
        let mut paged = session.paged(2000);
        let mut pager = paged
            .query("select * from akka_projection.entity");

        while let Ok(page) = &pager.next().await {
            for row in page {
                match Entity::try_from_row(row.clone()) {
                    Ok(e) => yield e,
                    Err(e) => println!("{:?}", e)
                }
            }
        }
    }
}

async fn insert_entity_change<
    T: CdrsTransport,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static
>(session: &Session<T, CM, LB>, row: EntityChange) -> Frame {
    let cql = "INSERT INTO akka_projection.entity_change (\
        entity_type, \
        tenant_id, \
        update_time_bucket, \
        update_time, \
        open_id) VALUES (?, ?, ?, ?, ?)";

    session.query_with_values(cql, row.into_query_values())
        .await
        .expect("insert")
}