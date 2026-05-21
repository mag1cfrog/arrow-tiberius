use super::{BenchClient, WriterBenchError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConnectionSnapshot {
    pub(super) net_transport: String,
    pub(super) protocol_type: String,
    pub(super) encrypt_option: String,
    pub(super) net_packet_size: i32,
    pub(super) num_reads: i64,
    pub(super) num_writes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActivitySnapshot {
    pub(super) connection: ConnectionSnapshot,
    pub(super) request: Option<RequestSnapshot>,
    pub(super) waiting_tasks: Vec<WaitingTaskSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RequestSnapshot {
    pub(super) status: String,
    pub(super) command: String,
    pub(super) wait_type: Option<String>,
    pub(super) wait_time_ms: i64,
    pub(super) last_wait_type: String,
    pub(super) wait_resource: String,
    pub(super) blocking_session_id: i32,
    pub(super) cpu_time_ms: i32,
    pub(super) total_elapsed_time_ms: i32,
    pub(super) reads: i64,
    pub(super) writes: i64,
    pub(super) logical_reads: i64,
    pub(super) open_transaction_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WaitingTaskSnapshot {
    pub(super) exec_context_id: i32,
    pub(super) wait_type: String,
    pub(super) wait_duration_ms: i64,
    pub(super) blocking_session_id: Option<i32>,
    pub(super) resource_description: Option<String>,
}

pub(super) async fn current_activity_snapshot(
    observer: &mut BenchClient,
    writer_session_id: i32,
) -> Result<ActivitySnapshot, WriterBenchError> {
    Ok(ActivitySnapshot {
        connection: connection_snapshot(observer, writer_session_id).await?,
        request: request_snapshot(observer, writer_session_id).await?,
        waiting_tasks: waiting_task_snapshots(observer, writer_session_id).await?,
    })
}

pub(super) async fn connection_snapshot(
    observer: &mut BenchClient,
    writer_session_id: i32,
) -> Result<ConnectionSnapshot, WriterBenchError> {
    let row = observer
        .simple_query(connection_snapshot_query(writer_session_id))
        .await
        .map_err(WriterBenchError::Tiberius)?
        .into_row()
        .await
        .map_err(WriterBenchError::Tiberius)?
        .ok_or_else(|| {
            WriterBenchError::Validation(format!(
                "SQL Server connection snapshot found no connection for writer session id {writer_session_id}"
            ))
        })?;

    Ok(ConnectionSnapshot {
        net_transport: required_string(&row, "net_transport")?,
        protocol_type: required_string(&row, "protocol_type")?,
        encrypt_option: required_string(&row, "encrypt_option")?,
        net_packet_size: required_i32(&row, "net_packet_size")?,
        num_reads: required_i64(&row, "num_reads")?,
        num_writes: required_i64(&row, "num_writes")?,
    })
}

async fn request_snapshot(
    observer: &mut BenchClient,
    writer_session_id: i32,
) -> Result<Option<RequestSnapshot>, WriterBenchError> {
    let Some(row) = observer
        .simple_query(request_snapshot_query(writer_session_id))
        .await
        .map_err(WriterBenchError::Tiberius)?
        .into_row()
        .await
        .map_err(WriterBenchError::Tiberius)?
    else {
        return Ok(None);
    };

    Ok(Some(RequestSnapshot {
        status: required_string(&row, "status")?,
        command: required_string(&row, "command")?,
        wait_type: optional_string(&row, "wait_type")?,
        wait_time_ms: required_i64(&row, "wait_time_ms")?,
        last_wait_type: required_string(&row, "last_wait_type")?,
        wait_resource: required_string(&row, "wait_resource")?,
        blocking_session_id: required_i32(&row, "blocking_session_id")?,
        cpu_time_ms: required_i32(&row, "cpu_time_ms")?,
        total_elapsed_time_ms: required_i32(&row, "total_elapsed_time_ms")?,
        reads: required_i64(&row, "reads")?,
        writes: required_i64(&row, "writes")?,
        logical_reads: required_i64(&row, "logical_reads")?,
        open_transaction_count: required_i32(&row, "open_transaction_count")?,
    }))
}

async fn waiting_task_snapshots(
    observer: &mut BenchClient,
    writer_session_id: i32,
) -> Result<Vec<WaitingTaskSnapshot>, WriterBenchError> {
    let rows = observer
        .simple_query(waiting_tasks_query(writer_session_id))
        .await
        .map_err(WriterBenchError::Tiberius)?
        .into_first_result()
        .await
        .map_err(WriterBenchError::Tiberius)?;

    rows.iter().map(waiting_task_from_row).collect()
}

fn waiting_task_from_row(row: &tiberius::Row) -> Result<WaitingTaskSnapshot, WriterBenchError> {
    Ok(WaitingTaskSnapshot {
        exec_context_id: required_i32(row, "exec_context_id")?,
        wait_type: required_string(row, "wait_type")?,
        wait_duration_ms: required_i64(row, "wait_duration_ms")?,
        blocking_session_id: optional_i32(row, "blocking_session_id")?,
        resource_description: optional_string(row, "resource_description")?,
    })
}

fn connection_snapshot_query(writer_session_id: i32) -> String {
    format!(
        "SELECT \
            CONVERT(nvarchar(60), c.net_transport) AS net_transport, \
            CONVERT(nvarchar(60), c.protocol_type) AS protocol_type, \
            CONVERT(nvarchar(60), c.encrypt_option) AS encrypt_option, \
            CONVERT(int, c.net_packet_size) AS net_packet_size, \
            CONVERT(bigint, c.num_reads) AS num_reads, \
            CONVERT(bigint, c.num_writes) AS num_writes \
        FROM sys.dm_exec_connections AS c \
        WHERE CONVERT(int, c.session_id) = {writer_session_id}"
    )
}

fn request_snapshot_query(writer_session_id: i32) -> String {
    format!(
        "SELECT \
            CONVERT(nvarchar(60), r.status) AS status, \
            CONVERT(nvarchar(60), r.command) AS command, \
            CONVERT(nvarchar(60), r.wait_type) AS wait_type, \
            CONVERT(bigint, r.wait_time) AS wait_time_ms, \
            CONVERT(nvarchar(60), r.last_wait_type) AS last_wait_type, \
            CONVERT(nvarchar(256), r.wait_resource) AS wait_resource, \
            CONVERT(int, r.blocking_session_id) AS blocking_session_id, \
            CONVERT(int, r.cpu_time) AS cpu_time_ms, \
            CONVERT(int, r.total_elapsed_time) AS total_elapsed_time_ms, \
            CONVERT(bigint, r.reads) AS reads, \
            CONVERT(bigint, r.writes) AS writes, \
            CONVERT(bigint, r.logical_reads) AS logical_reads, \
            CONVERT(int, r.open_transaction_count) AS open_transaction_count \
        FROM sys.dm_exec_requests AS r \
        WHERE CONVERT(int, r.session_id) = {writer_session_id}"
    )
}

fn waiting_tasks_query(writer_session_id: i32) -> String {
    format!(
        "SELECT \
            CONVERT(int, w.exec_context_id) AS exec_context_id, \
            CONVERT(nvarchar(60), w.wait_type) AS wait_type, \
            CONVERT(bigint, w.wait_duration_ms) AS wait_duration_ms, \
            CONVERT(int, w.blocking_session_id) AS blocking_session_id, \
            CONVERT(nvarchar(3072), w.resource_description) AS resource_description \
        FROM sys.dm_os_waiting_tasks AS w \
        WHERE CONVERT(int, w.session_id) = {writer_session_id} \
        ORDER BY w.exec_context_id, w.wait_type"
    )
}

fn required_i32(row: &tiberius::Row, column: &'static str) -> Result<i32, WriterBenchError> {
    row.try_get::<i32, _>(column)
        .map_err(WriterBenchError::Tiberius)?
        .ok_or_else(|| null_snapshot_column(column))
}

fn required_i64(row: &tiberius::Row, column: &'static str) -> Result<i64, WriterBenchError> {
    row.try_get::<i64, _>(column)
        .map_err(WriterBenchError::Tiberius)?
        .ok_or_else(|| null_snapshot_column(column))
}

fn required_string(row: &tiberius::Row, column: &'static str) -> Result<String, WriterBenchError> {
    row.try_get::<&str, _>(column)
        .map_err(WriterBenchError::Tiberius)?
        .map(ToOwned::to_owned)
        .ok_or_else(|| null_snapshot_column(column))
}

fn optional_i32(
    row: &tiberius::Row,
    column: &'static str,
) -> Result<Option<i32>, WriterBenchError> {
    row.try_get::<i32, _>(column)
        .map_err(WriterBenchError::Tiberius)
}

fn optional_string(
    row: &tiberius::Row,
    column: &'static str,
) -> Result<Option<String>, WriterBenchError> {
    row.try_get::<&str, _>(column)
        .map_err(WriterBenchError::Tiberius)
        .map(|value| value.map(ToOwned::to_owned))
}

fn null_snapshot_column(column: &'static str) -> WriterBenchError {
    WriterBenchError::Validation(format!(
        "SQL Server connection snapshot column `{column}` was null"
    ))
}

#[cfg(test)]
mod tests {
    use super::{connection_snapshot_query, request_snapshot_query, waiting_tasks_query};

    #[test]
    fn connection_snapshot_query_targets_one_writer_session() {
        let query = connection_snapshot_query(37);

        assert!(query.contains("FROM sys.dm_exec_connections AS c"));
        assert!(query.contains("WHERE CONVERT(int, c.session_id) = 37"));
        assert!(query.contains("AS net_packet_size"));
        assert!(query.contains("AS num_reads"));
        assert!(query.contains("AS num_writes"));
    }

    #[test]
    fn request_snapshot_query_targets_one_writer_session() {
        let query = request_snapshot_query(41);

        assert!(query.contains("FROM sys.dm_exec_requests AS r"));
        assert!(query.contains("WHERE CONVERT(int, r.session_id) = 41"));
        assert!(query.contains("AS wait_type"));
        assert!(query.contains("AS open_transaction_count"));
    }

    #[test]
    fn waiting_tasks_query_targets_one_writer_session() {
        let query = waiting_tasks_query(43);

        assert!(query.contains("FROM sys.dm_os_waiting_tasks AS w"));
        assert!(query.contains("WHERE CONVERT(int, w.session_id) = 43"));
        assert!(query.contains("AS wait_duration_ms"));
        assert!(query.contains("AS resource_description"));
    }
}
