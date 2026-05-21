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

fn null_snapshot_column(column: &'static str) -> WriterBenchError {
    WriterBenchError::Validation(format!(
        "SQL Server connection snapshot column `{column}` was null"
    ))
}

#[cfg(test)]
mod tests {
    use super::connection_snapshot_query;

    #[test]
    fn connection_snapshot_query_targets_one_writer_session() {
        let query = connection_snapshot_query(37);

        assert!(query.contains("FROM sys.dm_exec_connections AS c"));
        assert!(query.contains("WHERE CONVERT(int, c.session_id) = 37"));
        assert!(query.contains("AS net_packet_size"));
        assert!(query.contains("AS num_reads"));
        assert!(query.contains("AS num_writes"));
    }
}
