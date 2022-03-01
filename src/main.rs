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
use chrono::{DateTime, Duration, DurationRound, NaiveDateTime, Utc};
use futures_core::stream::Stream;
use futures::{stream, StreamExt};
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

#[derive(Debug)]
enum MigrateError {
    ConvertError(String),
    InsertError(String)
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
    let session_rc = Arc::new(session);

    let entity_stream = query_entity(session_rc.clone());

    tokio::pin!(entity_stream);

    let insert_session = &session_rc.clone();
    println!("Entity Start!");
    entity_stream
        .map(|entity| {
            get_time_bucket(entity.update_time)
                .map(|time_bucket| {
                    EntityChange {
                        entity_type: entity.entity_type,
                        tenant_id: entity.tenant_id,
                        update_time_bucket: time_bucket,
                        update_time: entity.update_time,
                        open_id: entity.open_id,
                    }
                })
        })
        .enumerate()
        .for_each_concurrent(Some(64), |(index, change_res)| async move {
            let inserted = match change_res {
                Ok(c) => {
                    let res = insert_entity_change(insert_session, &c).await;
                    println!("insert {}th entity {} {}", index, &c.entity_type, &c.open_id);
                    res
                },
                Err(e) => Err(e)
            };
            match inserted {
                Ok(_) => {},
                Err(reason) => println!("Failed: {:?}", reason)
            }
        })
        .await;
    println!("Entity Done!");

    let relation_stream = query_relation(session_rc.clone());

    tokio::pin!(relation_stream);

    println!("Relation Start!");
    relation_stream
        .flat_map(|relation| {
            let r1 = relation.clone();
            let row1 = RelationDirection {
                relation_type: r1.entity_type,
                relation_tenant: r1.tenant_id,
                direction: String::from("->"),
                l_tenant_id: r1.from_tenant,
                l_entity_type: r1.from_type,
                l_open_id: r1.from_open_id,
                r_tenant_id: r1.to_tenant,
                r_entity_type: r1.to_type,
                r_open_id: r1.to_open_id,
                open_id: r1.open_id
            };
            let row2 = RelationDirection {
                relation_type: relation.entity_type,
                relation_tenant: relation.tenant_id,
                direction: String::from("<-"),
                l_tenant_id: relation.to_tenant,
                l_entity_type: relation.to_type,
                l_open_id: relation.to_open_id,
                r_tenant_id: relation.from_tenant,
                r_entity_type: relation.from_type,
                r_open_id: relation.from_open_id,
                open_id: relation.open_id
            };
            stream::iter(vec![row1, row2])
        })
        .enumerate()
        .for_each_concurrent(Some(64), |(index, row)| async move {
            let idx = index;
            let r = row;
            let inserted = insert_relation_direction(insert_session, &r).await;
            println!("insert {}th relation {} {}", idx, &r.relation_type, &r.open_id);
            match inserted {
                Ok(_) => {},
                Err(reason) => println!("Failed: {:?}", reason)
            }
        })
        .await;
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

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct Relationship {
    entity_type: String,
    tenant_id: String,
    open_id: String,
    both_type: String,
    from_open_id: String,
    from_tenant: String,
    from_type: String,
    stages: HashMap<String, Stage>,
    to_open_id: String,
    to_tenant: String,
    to_type: String,
    create_time: i64,
    update_time: i64,
}

#[derive(Clone, Debug, IntoCdrsValue, TryFromRow, PartialEq)]
struct RelationDirection {
    relation_type: String,
    relation_tenant: String,
    direction: String,
    l_tenant_id: String,
    l_entity_type: String,
    l_open_id: String,
    r_tenant_id: String,
    r_entity_type: String,
    r_open_id: String,
    open_id: String
}

impl RelationDirection {
    fn into_query_values(self) -> QueryValues {
        query_values!(
            "relation_type" => self.relation_type,
            "relation_tenant" => self.relation_tenant,
            "direction" => self.direction,
            "l_tenant_id" => self.l_tenant_id,
            "l_entity_type" => self.l_entity_type,
            "l_open_id" => self.l_open_id,
            "r_tenant_id" => self.r_tenant_id,
            "r_entity_type" => self.r_entity_type,
            "r_open_id" => self.r_open_id,
            "open_id" => self.open_id
        )
    }
}


fn query_entity<
    T: CdrsTransport,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static
>(session: Arc<Session<T, CM, LB>>) -> impl Stream<Item = Entity> {
    stream! {
        let mut paged = session.paged(2000);
        let mut pager = paged
            .query("select * from akka_projection.entity");

        loop {
            let res = &pager.next().await;
            match res {
                Err(e) => println!("{}", e.to_string()),
                Ok(page) => {
                    for row in page {
                        match Entity::try_from_row(row.clone()) {
                            Ok(e) => yield e,
                            Err(e) => println!("{:?}", e)
                        }
                    }
                }
            }
            if !pager.has_more() {
                break;
            }
        }
    }
}

fn query_relation<
    T: CdrsTransport,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static
>(session: Arc<Session<T, CM, LB>>) -> impl Stream<Item = Relationship> {
    stream! {
        let mut paged = session.paged(2000);
        let mut pager = paged
            .query("select * from akka_projection.relationship");

        loop {
            let res = &pager.next().await;
            match res {
                Err(e) => println!("{}", e.to_string()),
                Ok(page) => {
                    for row in page {
                        match Relationship::try_from_row(row.clone()) {
                            Ok(e) => yield e,
                            Err(e) => println!("{:?}", e)
                        }
                    }
                }
            }
            if !pager.has_more() {
                break;
            }
        }
    }
}

async fn insert_entity_change<
    T: CdrsTransport,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static
>(session: &Arc<Session<T, CM, LB>>, row: &EntityChange) -> std::result::Result<Frame, MigrateError> {
    let cql = "INSERT INTO akka_projection.entity_change (\
        entity_type, \
        tenant_id, \
        update_time_bucket, \
        update_time, \
        open_id) VALUES (?, ?, ?, ?, ?)";

    session.query_with_values(cql, row.clone().into_query_values())
        .await
        .map_err(|e| MigrateError::InsertError(format!("{:?}", e)))
}

async fn insert_relation_direction<
    T: CdrsTransport,
    CM: ConnectionManager<T>,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync + 'static
>(session: &Session<T, CM, LB>, row: &RelationDirection) -> std::result::Result<Frame, MigrateError> {
    let cql = "INSERT INTO akka_projection.relation_direction (\
        relation_type, \
        relation_tenant, \
        direction, \
        l_tenant_id, \
        l_entity_type, \
        l_open_id, \
        r_tenant_id, \
        r_entity_type, \
        r_open_id,\
        open_id) VALUES (?, ?, ?, ?, ?)";

    session.query_with_values(cql, row.clone().into_query_values())
        .await
        .map_err(|e| MigrateError::InsertError(format!("{:?}", e)))
}

fn get_time_bucket(time: i64) -> std::result::Result<i64, MigrateError> {
    let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(time / 1000, (time % 1000 * 1_000_000) as u32), Utc);
    dt.duration_trunc(Duration::hours(1))
        .map(|x| x.timestamp_millis())
        .map_err(|e| MigrateError::ConvertError(e.to_string()))
}

#[test]
fn test_get_time_bucket() {
    let res = get_time_bucket(1642676614825);
    println!("{}", res.unwrap());
}