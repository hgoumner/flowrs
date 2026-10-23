use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use flowrs_airflow::client::dag_filter::DagFilter;
use flowrs_airflow::client::{v1, v2};
use flowrs_config::{AirflowAuth, AirflowConfig, AirflowVersion, BasicAuth};
use serde_json::{json, Value};
use url::Url;

use crate::airflow::client::FlowrsClient;

fn dag_response(version: &AirflowVersion, dag_id: &str) -> Value {
    match version {
        AirflowVersion::V2 => serde_json::to_value(v1::model::dag::DagResponse {
            dag_id: dag_id.to_string(),
            ..Default::default()
        })
        .unwrap(),
        AirflowVersion::V3 => serde_json::to_value(v2::model::dag::Dag {
            dag_id: dag_id.to_string(),
            ..Default::default()
        })
        .unwrap(),
    }
}

#[tokio::test]
async fn filters_every_page_and_refresh_for_both_api_versions() {
    for version in [AirflowVersion::V2, AirflowVersion::V3] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server_version = version.clone();
        let server = thread::spawn(move || {
            // Two pages on the first load, and the same two pages on refresh.
            for offset in [0, 50, 0, 50] {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let target = request_line.split_whitespace().nth(1).unwrap();
                let url = Url::parse(&format!("http://localhost{target}")).unwrap();
                assert_eq!(url.path(), format!("/{}/dags", server_version.api_path()));
                let query: Vec<_> = url.query_pairs().into_owned().collect();
                for (key, value) in [
                    ("limit", "50".to_string()),
                    ("offset", offset.to_string()),
                    ("dag_id_pattern", "team_%".to_string()),
                    ("tags", "finance & reporting".to_string()),
                ] {
                    assert!(query.contains(&(key.to_string(), value)));
                }
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                let dags = if offset == 0 {
                    // SQL '_' may return false positives; they must not enter
                    // the cache, nor stop pagination before later matches.
                    (0..50)
                        .map(|_| dag_response(&server_version, "teamXunrelated"))
                        .collect::<Vec<_>>()
                } else {
                    vec![dag_response(&server_version, "team_orders")]
                };
                let body = json!({"dags": dags, "total_entries": 51}).to_string();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let config = AirflowConfig {
            name: "test".to_string(),
            endpoint,
            auth: AirflowAuth::Basic(BasicAuth {
                username: "test".to_string(),
                password: "test".to_string(),
            }),
            managed: None,
            version,
            timeout_secs: 10,
            insecure: false,
        };
        let client = FlowrsClient::new(&config).unwrap();
        let filter = DagFilter::new(
            Some("team_*".to_string()),
            Some("finance & reporting".to_string()),
        )
        .unwrap();
        for _ in 0..2 {
            let result = client.list_dags_filtered(&filter).await.unwrap();
            assert_eq!(result.total_entries, 1);
            assert_eq!(result.dags.len(), 1);
            assert_eq!(result.dags[0].dag_id.as_ref(), "team_orders");
        }
        server.join().unwrap();
    }
}
