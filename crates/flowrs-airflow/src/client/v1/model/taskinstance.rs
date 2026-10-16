use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskInstanceCollectionResponse {
    pub task_instances: Vec<TaskInstanceResponse>,
    pub total_entries: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskInstanceResponse {
    pub task_id: String,
    pub task_display_name: Option<String>,
    pub dag_id: String,
    pub dag_run_id: String,
    #[serde(with = "time::serde::iso8601")]
    pub execution_date: OffsetDateTime,
    #[serde(with = "time::serde::iso8601::option")]
    pub start_date: Option<OffsetDateTime>,
    #[serde(with = "time::serde::iso8601::option")]
    pub end_date: Option<OffsetDateTime>,
    pub duration: Option<f64>,
    pub state: Option<String>,
    pub try_number: u32,
    pub map_index: i64,
    pub max_tries: i64,
    pub hostname: String,
    pub unixname: String,
    pub pool: String,
    pub pool_slots: i64,
    pub queue: Option<String>,
    pub priority_weight: Option<i64>,
    pub operator: Option<String>,
    #[serde(with = "time::serde::iso8601::option")]
    pub queued_when: Option<OffsetDateTime>,
    pub pid: Option<i64>,
    pub executor: Option<String>,
    pub executor_config: Option<serde_json::Value>,
    pub sla_miss: Option<serde_json::Value>,
    pub rendered_map_index: Option<String>,
    pub rendered_fields: serde_json::Value,
    pub trigger: Option<TriggerResponse>,
    pub triggerer_job: Option<JobResponse>,
    pub note: Option<String>,
}

/// Response model for a single task instance try from the tries endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskInstanceTryResponse {
    pub task_id: String,
    pub dag_id: String,
    pub dag_run_id: String,
    #[serde(with = "time::serde::iso8601::option")]
    pub start_date: Option<OffsetDateTime>,
    #[serde(with = "time::serde::iso8601::option")]
    pub end_date: Option<OffsetDateTime>,
    pub duration: Option<f64>,
    pub state: Option<String>,
    pub try_number: u32,
    pub map_index: i64,
    #[serde(default, with = "time::serde::iso8601::option")]
    pub queued_when: Option<OffsetDateTime>,
}

/// Response model for a collection of task instance tries.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskInstanceTriesResponse {
    pub task_instances: Vec<TaskInstanceTryResponse>,
    pub total_entries: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerResponse {
    pub id: i64,
    pub classpath: String,
    pub kwargs: String,
    #[serde(with = "time::serde::iso8601::option")]
    pub created_date: Option<OffsetDateTime>,
    pub triggerer_id: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResponse {
    pub id: i64,
    pub dag_id: Option<String>,
    pub state: String,
    pub job_type: String,
    #[serde(with = "time::serde::iso8601::option")]
    pub start_date: Option<OffsetDateTime>,
    #[serde(with = "time::serde::iso8601::option")]
    pub end_date: Option<OffsetDateTime>,
    #[serde(with = "time::serde::iso8601::option")]
    pub latest_heartbeat: Option<OffsetDateTime>,
    pub executor_class: Option<String>,
    pub hostname: String,
    pub unixname: String,
}

#[cfg(test)]
mod tests {
    use super::TaskInstanceCollectionResponse;
    use serde_json::json;

    #[test]
    fn task_instances_accept_nullable_triggerer_job_fields() {
        let mut response = json!({
            "task_instances": [{
                "task_id": "waiting_task",
                "dag_id": "example_dag",
                "dag_run_id": "scheduled__2026-10-15T02:00:00+00:00",
                "execution_date": "2026-10-15T02:00:00Z",
                "start_date": null,
                "end_date": null,
                "duration": null,
                "state": "deferred",
                "try_number": 1,
                "map_index": -1,
                "max_tries": 0,
                "hostname": "",
                "unixname": "",
                "pool": "default_pool",
                "pool_slots": 1,
                "queued_when": null,
                "rendered_fields": {},
                "triggerer_job": {
                    "id": 1,
                    "dag_id": null,
                    "state": "running",
                    "job_type": "TriggererJob",
                    "start_date": "2026-10-15T01:00:00Z",
                    "end_date": null,
                    "latest_heartbeat": "2026-10-15T02:00:00Z",
                    "executor_class": null,
                    "hostname": "triggerer",
                    "unixname": "airflow"
                }
            }],
            "total_entries": 1
        });
        let parsed: TaskInstanceCollectionResponse =
            serde_json::from_value(response.clone()).unwrap();
        let job = parsed.task_instances[0].triggerer_job.as_ref().unwrap();
        assert_eq!(job.dag_id, None);
        assert_eq!(job.executor_class, None);

        response["task_instances"][0]["triggerer_job"]["dag_id"] = json!("example_dag");
        response["task_instances"][0]["triggerer_job"]["executor_class"] = json!("LocalExecutor");
        let parsed: TaskInstanceCollectionResponse =
            serde_json::from_value(response.clone()).unwrap();
        let job = parsed.task_instances[0].triggerer_job.as_ref().unwrap();
        assert_eq!(job.dag_id.as_deref(), Some("example_dag"));
        assert_eq!(job.executor_class.as_deref(), Some("LocalExecutor"));

        response["task_instances"][0]["triggerer_job"] = json!(null);
        let parsed: TaskInstanceCollectionResponse = serde_json::from_value(response).unwrap();
        assert!(parsed.task_instances[0].triggerer_job.is_none());
    }
}
