#![cfg(feature = "session")]

use std::{
    error::Error as _,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

use arrow_array::{BooleanArray, Decimal128Array};
use arrow_schema::ArrowError;
use futures_util::TryStreamExt;
use serde_json::{Value, json};
use snowflake_connector_rs::{AuthConfig, Client, ClientConfig, EndpointConfig, ErrorKind};
use snowpark_rs::{
    CompileError, DataFrame, DataType, Projection, Session, col, lit, session::ExecuteError, sum,
};

const LOGIN: &str = r#"{"success":true,"data":{"token":"test-token"}}"#;
const NULL_RESULT: &str = r#"{"success":true,"data":{"queryId":"null-query","rowset":[["1","42"],[null,"42"],["0","42"]],"rowtype":[{"name":"value?","nullable":true,"type":"boolean"},{"name":"ANSWER","nullable":false,"precision":38,"scale":0,"type":"fixed"}],"queryResultFormat":"json"}}"#;
const SERVER_ERROR: &str = r#"{"code":"000904","message":"SQL compilation error: invalid identifier","success":false,"data":{"queryId":"server-query"}}"#;
const CELL_ERROR: &str = r#"{"success":true,"data":{"queryId":"decode-query","rowset":[["not-a-boolean"]],"rowtype":[{"name":"FLAG","nullable":true,"type":"boolean"}],"queryResultFormat":"json"}}"#;
const ARROW_ERROR: &str = r#"{"success":true,"data":{"queryId":"arrow-query","rowset":[["100"]],"rowtype":[{"name":"AMOUNT","nullable":true,"precision":2,"scale":0,"type":"fixed"}],"queryResultFormat":"json"}}"#;

// Exercise the actual connector and adapter over loopback without a Snowflake
// account. Return every request so tests can also check that no SQL was sent
// for a locally invalid plan.
fn with_responses(responses: &[&'static str], test: impl AsyncFnOnce(Session)) -> Vec<Value> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let responses: Vec<_> = std::iter::once(LOGIN)
        .chain(responses.iter().copied())
        .collect();
    let server = thread::spawn(move || {
        responses.into_iter().map(|body| {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut size = None;
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    size = Some(value.trim().parse::<usize>().unwrap());
                }
            }
            let mut request = vec![0; size.unwrap()];
            reader.read_exact(&mut request).unwrap();
            let request = serde_json::from_slice::<Value>(&request).unwrap();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            request
        }).collect::<Vec<_>>()
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(15), async {
            let session = Client::new(
                ClientConfig::new(
                    "test-user",
                    "test-account",
                    AuthConfig::password("test-password"),
                )
                .with_endpoint(EndpointConfig::custom_base_url(endpoint.parse().unwrap())),
            )
            .unwrap()
            .create_session()
            .await
            .unwrap();
            test(session).await;
        })
        .await
        .unwrap();
    });
    server.join().unwrap().into_iter().skip(1).collect()
}

#[test]
fn compilation_errors_never_submit_and_quoted_question_marks_keep_bind_order() {
    let source = DataFrame::table_name(r#""Table?""#).unwrap();
    let left = source.select(["id", "flag"]);
    let right = source.select(["id", "flag"]);
    let condition = left.col("id").eq(right.col("id"));
    let invalid = [
        #[cfg(feature = "polars")]
        (
            source.select_polars([
                snowpark_rs::polars::col("ID").floor_div(snowpark_rs::polars::lit(2.5))
            ]),
            CompileError::UnsupportedExpression {
                operation: "select",
                node: "BinaryExpr.FloorDivide".into(),
                reason: "floor division requires integer operands".into(),
            },
        ),
        (
            source.select(Vec::<Projection>::new()),
            CompileError::EmptyProjection,
        ),
        (
            source.select(["bad.name"]),
            CompileError::InvalidIdentifier("bad.name".into()),
        ),
        (
            left.select(["missing"]),
            CompileError::UnknownColumn("missing".into()),
        ),
        (
            source.select(["id", r#""ID""#]).select(["id"]),
            CompileError::AmbiguousColumn("id".into()),
        ),
        (
            left.join(&right, source.col("id").eq(right.col("id"))),
            CompileError::OutOfScopeColumn("id".into()),
        ),
        (
            source.agg(Vec::<Projection>::new()),
            CompileError::EmptyAggregate,
        ),
        (source.agg(["id"]), CompileError::ExpectedAggregate),
        (
            source.select([sum("id")]),
            CompileError::UnexpectedAggregate,
        ),
        (
            source.sort(["id"]).select(["id"]),
            CompileError::IntermediateSort,
        ),
        (
            left.join(&source, left.col("id").eq(source.col("id"))),
            CompileError::UnnamedJoinInput,
        ),
        (
            left.join(&left, left.col("id").eq(left.col("id"))),
            CompileError::IdenticalJoinInputs,
        ),
        (
            left.join_with_suffixes(&right, condition, "_same", "_same"),
            CompileError::DuplicateJoinOutput("\"ID_SAME\"".into()),
        ),
        (
            source.select([col("id")
                .cast(DataType::Number {
                    precision: 39,
                    scale: 0,
                })
                .alias("x")]),
            CompileError::InvalidDataType("NUMBER(39,0)".into()),
        ),
        (
            source.union_by_name(&left, false),
            CompileError::UnknownSchema("union by name"),
        ),
        (left.rename(lit(1), "x"), CompileError::ExpectedReference),
        (
            left.rename("flag", "id"),
            CompileError::DuplicateOutput("\"ID\"".into()),
        ),
        (
            left.union(&right.select(["id"])),
            CompileError::UnionArity { left: 2, right: 1 },
        ),
        (
            left.union_by_name(&source.select(["id", "other"]), false),
            CompileError::UnionNames,
        ),
        (
            source.filter(left.membership(col("id"))),
            CompileError::MembershipArity(2),
        ),
    ];
    let valid = source.filter(col(r#""label?""#).eq(lit("first?"))).select([
        col(r#""value?""#).alias(r#""value?""#),
        lit(42).alias("answer"),
    ]);
    let requests = with_responses(&[NULL_RESULT], async |session| {
        for (frame, expected) in invalid {
            assert!(
                matches!(frame.to_arrow(&session).await, Err(ExecuteError::Compile(actual)) if actual == expected)
            );
            assert!(
                matches!(frame.to_arrow_batches(&session).await, Err(ExecuteError::Compile(actual)) if actual == expected)
            );
            assert!(
                matches!(frame.schema_with_session(&session).await, Err(ExecuteError::Compile(actual)) if actual == expected)
            );
        }
        let result = valid.to_arrow(&session).await.unwrap();
        let flags = result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(
            flags.iter().collect::<Vec<_>>(),
            [Some(true), None, Some(false)]
        );
        let answers = result
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(answers.iter().collect::<Vec<_>>(), [Some(42); 3]);
    });
    let [request] = requests.as_slice() else {
        panic!("expected one valid request")
    };
    assert_eq!(request["sqlText"], valid.to_sql().unwrap().sql);
    assert_eq!(
        request["bindings"],
        json!({
            "1": {"type":"FIXED","value":"42"},
            "2": {"type":"TEXT","value":"first?"}
        })
    );
}

#[test]
fn execution_errors_preserve_their_stage_query_id_and_source() {
    let frame = DataFrame::table_name("T").unwrap();
    with_responses(&[SERVER_ERROR, CELL_ERROR, ARROW_ERROR], async |session| {
        let error = frame.to_arrow(&session).await.unwrap_err();
        let ExecuteError::Submit(ref source) = error else {
            panic!("{error:?}")
        };
        assert_eq!(source.kind(), ErrorKind::Server);
        assert_eq!(source.snowflake_code(), Some("000904"));
        assert_eq!(source.query_id(), Some("server-query"));
        assert_eq!(
            source.snowflake_message(),
            Some("SQL compilation error: invalid identifier")
        );
        assert!(
            error
                .source()
                .unwrap()
                .is::<snowflake_connector_rs::Error>()
        );
        assert!(error.to_string().contains("server-query"));

        let batches = frame.to_arrow_batches(&session).await.unwrap();
        assert_eq!(batches.query_id(), "decode-query");
        let error = batches.try_collect::<Vec<_>>().await.unwrap_err();
        let ExecuteError::Read {
            ref query_id,
            ref source,
        } = error
        else {
            panic!("{error:?}")
        };
        assert_eq!(query_id, "decode-query");
        assert_eq!(source.kind(), ErrorKind::Decode);
        let cell = source.as_cell_decode_error().unwrap();
        assert_eq!(cell.column_name(), "FLAG");
        assert_eq!(cell.row_index(), 0);
        assert!(
            error
                .source()
                .unwrap()
                .is::<snowflake_connector_rs::Error>()
        );
        assert!(error.to_string().contains("decode-query"));

        let error = frame.to_arrow(&session).await.unwrap_err();
        let ExecuteError::Arrow {
            ref query_id,
            ref source,
        } = error
        else {
            panic!("{error:?}")
        };
        assert_eq!(query_id, "arrow-query");
        assert!(matches!(source, ArrowError::CastError(message) if message.contains("AMOUNT")));
        assert!(error.source().unwrap().is::<ArrowError>());
        assert!(error.to_string().contains("arrow-query"));
    });
}
