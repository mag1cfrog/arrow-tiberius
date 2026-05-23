//! Direct raw TDS bulk encoder internals.
#![allow(dead_code)]

use arrow_array::{
    BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array,
};

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, Error, Result, SchemaMapping,
    conversion::arrow_to_mssql::{
        primitive::PrimitiveArrowToMssql, variable_width::VariableWidthArrowToMssql,
    },
    write::record_batch::validate_runtime_columns,
};

pub(crate) mod layout;
pub(crate) mod payload;
pub(crate) mod plan;
pub(crate) mod primitive;
pub(crate) mod variable_width;

use payload::EncodedRowsPayload;
use plan::{CurrentDirectMappings, DirectColumnEncoding, DirectEncoderPlan};
use primitive::{
    allocate_rows_payload_with_tokens, append_boolean_cell, append_float32_cell,
    append_float64_cell, append_int8_cell, append_int16_cell, append_int32_cell, append_int64_cell,
    append_uint8_cell, append_uint16_cell, append_uint32_cell, build_fixed_width_row_layout,
    build_fixed_width_row_range_layout, fill_boolean_column, fill_float32_column,
    fill_float64_column, fill_int8_column, fill_int16_column, fill_int32_column, fill_int64_column,
    fill_uint8_column, fill_uint16_column, fill_uint32_column,
    measure_primitive_column_cell_lengths, try_encode_fixed_width_primitive_rows,
};
use variable_width::{
    append_nvarchar_cell, append_varbinary_cell, fill_nvarchar_column, fill_varbinary_column,
    measure_variable_width_column_cell_lengths,
};

/// Direct raw TDS encoder facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectEncoder {
    mappings: Vec<SchemaMapping>,
    plan: DirectEncoderPlan,
}

impl DirectEncoder {
    /// Creates a direct encoder using the current supported direct mappings.
    pub(crate) fn new(mappings: &[SchemaMapping]) -> Result<Self> {
        Self::new_with_support(mappings, &CurrentDirectMappings)
    }

    /// Creates a direct encoder using an explicit support checker.
    pub(crate) fn new_with_support(
        mappings: &[SchemaMapping],
        support: &impl plan::DirectEncoderSupport,
    ) -> Result<Self> {
        Ok(Self {
            mappings: mappings.to_vec(),
            plan: DirectEncoderPlan::new(mappings, support)?,
        })
    }

    /// Returns the checked schema mappings consumed by this encoder.
    pub(crate) fn mappings(&self) -> &[SchemaMapping] {
        &self.mappings
    }

    /// Returns the checked direct encoder plan.
    pub(crate) const fn plan(&self) -> &DirectEncoderPlan {
        &self.plan
    }

    /// Returns true when this encoder contains at least one variable-width column.
    pub(crate) fn has_variable_width_column(&self) -> bool {
        self.plan
            .columns()
            .iter()
            .any(|column| matches!(column.encoding(), DirectColumnEncoding::VariableWidth(_)))
    }

    /// Encodes a runtime batch into complete raw TDS row payload bytes.
    pub(crate) fn encode_batch(&self, batch: &RecordBatch) -> Result<EncodedRowsPayload> {
        self.encode_checked_batch(batch)
    }

    /// Measures and validates a runtime batch without allocating encoded bytes.
    pub(crate) fn measure_batch(&self, batch: &RecordBatch) -> Result<MeasuredDirectBatch> {
        validate_runtime_columns(batch, &self.mappings)?;

        let row_count = batch.num_rows();
        let column_count = self.plan.column_count();

        if row_count == 0 {
            return Ok(MeasuredDirectBatch::empty(column_count));
        }

        let cell_lengths = self.measure_cell_lengths(batch)?;
        MeasuredDirectBatch::new(row_count, column_count, cell_lengths)
    }

    /// Encodes a contiguous row range from a runtime batch.
    ///
    /// Returned row-token offsets are relative to the returned payload, so the
    /// first non-empty range always starts at offset zero.
    pub(crate) fn encode_batch_range(
        &self,
        batch: &RecordBatch,
        start_row: usize,
        row_count: usize,
    ) -> Result<EncodedRowsPayload> {
        let end_row = start_row
            .checked_add(row_count)
            .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;
        if end_row > batch.num_rows() {
            return Err(invalid_payload(format!(
                "direct row range {start_row}..{end_row} is outside batch row count {}",
                batch.num_rows()
            )));
        }

        let batch = batch.slice(start_row, row_count);
        self.encode_checked_batch(&batch)
    }

    /// Encodes one range from a pre-measured direct batch.
    pub(crate) fn encode_measured_batch_range(
        &self,
        batch: &RecordBatch,
        measured: &MeasuredDirectBatch,
        start_row: usize,
        row_count: usize,
    ) -> Result<EncodedRowsPayload> {
        measured.check_range(start_row, row_count)?;

        if row_count == 0 {
            return EncodedRowsPayload::new(Vec::new(), Vec::new());
        }

        if measured.row_count() != batch.num_rows() {
            return Err(invalid_payload(format!(
                "measured row count {} does not match runtime batch row count {}",
                measured.row_count(),
                batch.num_rows()
            )));
        }

        if measured.column_count() != self.plan.column_count() {
            return Err(invalid_payload(format!(
                "measured column count {} does not match direct plan column count {}",
                measured.column_count(),
                self.plan.column_count()
            )));
        }

        let layout = measured.range_layout(start_row, row_count)?;
        let batch = batch.slice(start_row, row_count);
        let mut bytes = allocate_rows_payload_with_tokens(&layout);
        self.fill_columns(&batch, &layout, &mut bytes)?;

        EncodedRowsPayload::new(bytes, layout.row_token_offsets().to_vec())
    }

    /// Encodes one measured range directly into a Tiberius raw rows buffer.
    pub(crate) fn encode_measured_batch_range_into(
        &self,
        batch: &RecordBatch,
        measured: &MeasuredDirectBatch,
        start_row: usize,
        row_count: usize,
        buf: &mut tiberius::RawRowsAppendBuffer<'_>,
    ) -> Result<tiberius::RawRowsAppend> {
        measured.check_range(start_row, row_count)?;

        if measured.row_count() != batch.num_rows() {
            return Err(invalid_payload(format!(
                "measured row count {} does not match runtime batch row count {}",
                measured.row_count(),
                batch.num_rows()
            )));
        }

        if measured.column_count() != self.plan.column_count() {
            return Err(invalid_payload(format!(
                "measured column count {} does not match direct plan column count {}",
                measured.column_count(),
                self.plan.column_count()
            )));
        }

        let runtime_columns = self.runtime_columns(batch)?;
        let mut row_token_offsets = Vec::with_capacity(row_count);
        let mut written = 0usize;

        let end_row = start_row
            .checked_add(row_count)
            .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;

        for row_index in start_row..end_row {
            row_token_offsets.push(written);
            buf.put_u8(payload::TDS_ROW_TOKEN);
            written = checked_add(written, 1)?;

            for (column_index, column) in runtime_columns.iter().enumerate() {
                let measured_len = measured.cell_len(row_index, column_index)?;
                column.append_cell(buf, row_index, measured_len)?;
                written = checked_add(written, measured_len)?;
            }
        }

        Ok(tiberius::RawRowsAppend::new(row_token_offsets))
    }

    fn encode_checked_batch(&self, batch: &RecordBatch) -> Result<EncodedRowsPayload> {
        validate_runtime_columns(batch, &self.mappings)?;

        if self.plan.is_empty() && batch.num_rows() == 0 {
            return EncodedRowsPayload::new(Vec::new(), Vec::new());
        }

        if let Some(payload) = try_encode_fixed_width_primitive_rows(batch, self.plan.columns())? {
            return Ok(payload);
        }

        let layout = self.measure_layout(batch)?;
        let mut bytes = allocate_rows_payload_with_tokens(&layout);
        self.fill_columns(batch, &layout, &mut bytes)?;

        EncodedRowsPayload::new(bytes, layout.row_token_offsets().to_vec())
    }

    fn measure_layout(&self, batch: &RecordBatch) -> Result<layout::RowLayout> {
        let row_count = batch.num_rows();
        if row_count == 0 {
            return layout::RowLayout::new(Vec::new(), Vec::new(), Vec::new(), 0);
        }

        let cell_lengths = self.measure_cell_lengths(batch)?;
        build_fixed_width_row_layout(row_count, self.plan.column_count(), &cell_lengths)
    }

    fn measure_cell_lengths(&self, batch: &RecordBatch) -> Result<Vec<usize>> {
        let row_count = batch.num_rows();
        let column_count = self.plan.column_count();

        if row_count == 0 {
            return Ok(Vec::new());
        }

        let mut cell_lengths = vec![0; row_count * column_count];

        for (column_index, column) in self.plan.columns().iter().enumerate() {
            let Some(array) = batch
                .columns()
                .get(column.source_index())
                .map(AsRef::as_ref)
            else {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    0,
                    DiagnosticCode::ValueTypeMismatch,
                    "planned direct column index is outside the runtime batch",
                )));
            };

            match column.encoding() {
                DirectColumnEncoding::Primitive(_) => {
                    measure_primitive_column_cell_lengths(
                        array,
                        column,
                        column_index,
                        column_count,
                        &mut cell_lengths,
                    )?;
                }
                DirectColumnEncoding::VariableWidth(_) => {
                    measure_variable_width_column_cell_lengths(
                        array,
                        column,
                        column_index,
                        column_count,
                        &mut cell_lengths,
                    )?;
                }
            }
        }

        Ok(cell_lengths)
    }

    fn fill_columns(
        &self,
        batch: &RecordBatch,
        layout: &layout::RowLayout,
        bytes: &mut [u8],
    ) -> Result<()> {
        let column_count = self.plan.column_count();

        for (column_index, column) in self.plan.columns().iter().enumerate() {
            let Some(array) = batch
                .columns()
                .get(column.source_index())
                .map(AsRef::as_ref)
            else {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    0,
                    DiagnosticCode::ValueTypeMismatch,
                    "planned direct column index is outside the runtime batch",
                )));
            };

            match column.encoding() {
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => {
                    let array = downcast_direct_array::<BooleanArray>(array, column)?;
                    fill_boolean_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => {
                    let array = downcast_direct_array::<UInt8Array>(array, column)?;
                    fill_uint8_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int8ToSmallInt) => {
                    let array = downcast_direct_array::<Int8Array>(array, column)?;
                    fill_int8_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int16ToSmallInt) => {
                    let array = downcast_direct_array::<Int16Array>(array, column)?;
                    fill_int16_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
                    let array = downcast_direct_array::<Int32Array>(array, column)?;
                    fill_int32_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => {
                    let array = downcast_direct_array::<UInt16Array>(array, column)?;
                    fill_uint16_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
                    let array = downcast_direct_array::<Int64Array>(array, column)?;
                    fill_int64_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => {
                    let array = downcast_direct_array::<UInt32Array>(array, column)?;
                    fill_uint32_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => {
                    let array = downcast_direct_array::<Float32Array>(array, column)?;
                    fill_float32_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
                    let array = downcast_direct_array::<Float64Array>(array, column)?;
                    fill_float64_column(array, column, column_index, column_count, layout, bytes)?;
                }
                DirectColumnEncoding::Primitive(other) => {
                    return Err(unsupported_batch(format!(
                        "direct primitive fill is not implemented yet for {other:?}"
                    )));
                }
                DirectColumnEncoding::VariableWidth(other) => match other {
                    VariableWidthArrowToMssql::Utf8ToNVarChar { .. } => {
                        let array = downcast_direct_array::<StringArray>(array, column)?;
                        fill_nvarchar_column(
                            array,
                            column,
                            column_index,
                            column_count,
                            layout,
                            bytes,
                        )?;
                    }
                    VariableWidthArrowToMssql::BinaryToVarBinary { .. } => {
                        let array = downcast_direct_array::<BinaryArray>(array, column)?;
                        fill_varbinary_column(
                            array,
                            column,
                            column_index,
                            column_count,
                            layout,
                            bytes,
                        )?;
                    }
                    unsupported => {
                        return Err(unsupported_batch(format!(
                            "direct variable-width fill is not implemented yet for {unsupported:?}"
                        )));
                    }
                },
            }
        }

        Ok(())
    }

    fn runtime_columns<'a>(
        &'a self,
        batch: &'a RecordBatch,
    ) -> Result<Vec<RuntimeDirectColumn<'a>>> {
        let mut columns = Vec::with_capacity(self.plan.column_count());

        for column in self.plan.columns() {
            let Some(array) = batch
                .columns()
                .get(column.source_index())
                .map(AsRef::as_ref)
            else {
                return Err(value_conversion_error(row_column_diagnostic(
                    column,
                    0,
                    DiagnosticCode::ValueTypeMismatch,
                    "planned direct column index is outside the runtime batch",
                )));
            };

            let runtime = match column.encoding() {
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::BooleanToBit) => {
                    RuntimeDirectColumn::Boolean {
                        column,
                        array: downcast_direct_array::<BooleanArray>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt8ToTinyInt) => {
                    RuntimeDirectColumn::UInt8 {
                        column,
                        array: downcast_direct_array::<UInt8Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int8ToSmallInt) => {
                    RuntimeDirectColumn::Int8 {
                        column,
                        array: downcast_direct_array::<Int8Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int16ToSmallInt) => {
                    RuntimeDirectColumn::Int16 {
                        column,
                        array: downcast_direct_array::<Int16Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int32ToInt) => {
                    RuntimeDirectColumn::Int32 {
                        column,
                        array: downcast_direct_array::<Int32Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt16ToInt) => {
                    RuntimeDirectColumn::UInt16 {
                        column,
                        array: downcast_direct_array::<UInt16Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Int64ToBigInt) => {
                    RuntimeDirectColumn::Int64 {
                        column,
                        array: downcast_direct_array::<Int64Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::UInt32ToBigInt) => {
                    RuntimeDirectColumn::UInt32 {
                        column,
                        array: downcast_direct_array::<UInt32Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float32ToReal) => {
                    RuntimeDirectColumn::Float32 {
                        column,
                        array: downcast_direct_array::<Float32Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(PrimitiveArrowToMssql::Float64ToFloat) => {
                    RuntimeDirectColumn::Float64 {
                        column,
                        array: downcast_direct_array::<Float64Array>(array, column)?,
                    }
                }
                DirectColumnEncoding::Primitive(other) => {
                    return Err(unsupported_batch(format!(
                        "direct primitive append is not implemented yet for {other:?}"
                    )));
                }
                DirectColumnEncoding::VariableWidth(
                    VariableWidthArrowToMssql::Utf8ToNVarChar { .. },
                ) => RuntimeDirectColumn::Utf8 {
                    column,
                    array: downcast_direct_array::<StringArray>(array, column)?,
                },
                DirectColumnEncoding::VariableWidth(
                    VariableWidthArrowToMssql::BinaryToVarBinary { .. },
                ) => RuntimeDirectColumn::Binary {
                    column,
                    array: downcast_direct_array::<BinaryArray>(array, column)?,
                },
                DirectColumnEncoding::VariableWidth(other) => {
                    return Err(unsupported_batch(format!(
                        "direct variable-width append is not implemented yet for {other:?}"
                    )));
                }
            };

            columns.push(runtime);
        }

        Ok(columns)
    }
}

enum RuntimeDirectColumn<'a> {
    Boolean {
        column: &'a plan::DirectColumnPlan,
        array: &'a BooleanArray,
    },
    UInt8 {
        column: &'a plan::DirectColumnPlan,
        array: &'a UInt8Array,
    },
    Int8 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Int8Array,
    },
    Int16 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Int16Array,
    },
    Int32 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Int32Array,
    },
    UInt16 {
        column: &'a plan::DirectColumnPlan,
        array: &'a UInt16Array,
    },
    Int64 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Int64Array,
    },
    UInt32 {
        column: &'a plan::DirectColumnPlan,
        array: &'a UInt32Array,
    },
    Float32 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Float32Array,
    },
    Float64 {
        column: &'a plan::DirectColumnPlan,
        array: &'a Float64Array,
    },
    Utf8 {
        column: &'a plan::DirectColumnPlan,
        array: &'a StringArray,
    },
    Binary {
        column: &'a plan::DirectColumnPlan,
        array: &'a BinaryArray,
    },
}

impl RuntimeDirectColumn<'_> {
    fn append_cell(
        &self,
        buf: &mut tiberius::RawRowsAppendBuffer<'_>,
        row_index: usize,
        measured_len: usize,
    ) -> Result<()> {
        match self {
            Self::Boolean { column, array } => {
                append_boolean_cell(buf, array, column, row_index, measured_len)
            }
            Self::UInt8 { column, array } => {
                append_uint8_cell(buf, array, column, row_index, measured_len)
            }
            Self::Int8 { column, array } => {
                append_int8_cell(buf, array, column, row_index, measured_len)
            }
            Self::Int16 { column, array } => {
                append_int16_cell(buf, array, column, row_index, measured_len)
            }
            Self::Int32 { column, array } => {
                append_int32_cell(buf, array, column, row_index, measured_len)
            }
            Self::UInt16 { column, array } => {
                append_uint16_cell(buf, array, column, row_index, measured_len)
            }
            Self::Int64 { column, array } => {
                append_int64_cell(buf, array, column, row_index, measured_len)
            }
            Self::UInt32 { column, array } => {
                append_uint32_cell(buf, array, column, row_index, measured_len)
            }
            Self::Float32 { column, array } => {
                append_float32_cell(buf, array, column, row_index, measured_len)
            }
            Self::Float64 { column, array } => {
                append_float64_cell(buf, array, column, row_index, measured_len)
            }
            Self::Utf8 { column, array } => {
                append_nvarchar_cell(buf, array, column, row_index, measured_len)
            }
            Self::Binary { column, array } => {
                append_varbinary_cell(buf, array, column, row_index, measured_len)
            }
        }
    }
}

/// Direct row payload measurement for one runtime batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MeasuredDirectBatch {
    row_count: usize,
    column_count: usize,
    cell_lengths: Vec<usize>,
    row_lengths: Vec<usize>,
    payload_len: usize,
}

impl MeasuredDirectBatch {
    fn empty(column_count: usize) -> Self {
        Self {
            row_count: 0,
            column_count,
            cell_lengths: Vec::new(),
            row_lengths: Vec::new(),
            payload_len: 0,
        }
    }

    fn new(row_count: usize, column_count: usize, cell_lengths: Vec<usize>) -> Result<Self> {
        let expected_cell_count = row_count
            .checked_mul(column_count)
            .ok_or_else(|| invalid_payload("measured cell count overflowed usize"))?;
        if cell_lengths.len() != expected_cell_count {
            return Err(invalid_payload(format!(
                "measured cell length count {} does not match row count {row_count} and column count {column_count}",
                cell_lengths.len()
            )));
        }

        let (row_lengths, payload_len) =
            measure_row_lengths(row_count, column_count, &cell_lengths)?;

        Ok(Self {
            row_count,
            column_count,
            cell_lengths,
            row_lengths,
            payload_len,
        })
    }

    /// Returns the measured row count.
    pub(crate) const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the measured column count.
    pub(crate) const fn column_count(&self) -> usize {
        self.column_count
    }

    /// Returns the complete measured payload length.
    pub(crate) const fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Splits measured rows into payload ranges capped by byte length.
    pub(crate) fn row_ranges(&self, max_payload_bytes: usize) -> Result<Vec<MeasuredRowRange>> {
        if max_payload_bytes == 0 {
            return Err(invalid_payload(
                "direct row range byte limit must be greater than zero",
            ));
        }

        let mut ranges = Vec::new();
        let mut start = 0usize;
        let mut len = 0usize;
        let mut bytes = 0usize;

        for (row_index, row_len) in self.row_lengths.iter().copied().enumerate() {
            let next_bytes = bytes
                .checked_add(row_len)
                .ok_or_else(|| invalid_payload("measured row range length overflowed usize"))?;

            if len > 0 && next_bytes > max_payload_bytes {
                ranges.push(MeasuredRowRange { start, len });
                start = row_index;
                len = 0;
                bytes = row_len;
            } else {
                bytes = next_bytes;
            }

            len += 1;
        }

        if len > 0 {
            ranges.push(MeasuredRowRange { start, len });
        }

        Ok(ranges)
    }

    pub(crate) fn range_payload_len(&self, start_row: usize, row_count: usize) -> Result<usize> {
        self.check_range(start_row, row_count)?;

        let end_row = start_row
            .checked_add(row_count)
            .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;
        self.row_lengths[start_row..end_row]
            .iter()
            .try_fold(0usize, |total, row_len| {
                total
                    .checked_add(*row_len)
                    .ok_or_else(|| invalid_payload("measured row range length overflowed usize"))
            })
    }

    fn cell_len(&self, row_index: usize, column_index: usize) -> Result<usize> {
        self.check_range(row_index, 1)?;

        if column_index >= self.column_count {
            return Err(invalid_payload(format!(
                "direct measured column index {column_index} is outside measured column count {}",
                self.column_count
            )));
        }

        let index = row_index
            .checked_mul(self.column_count)
            .and_then(|base| base.checked_add(column_index))
            .ok_or_else(|| invalid_payload("measured cell length index overflowed usize"))?;

        self.cell_lengths.get(index).copied().ok_or_else(|| {
            invalid_payload(format!(
                "measured cell length index {index} is outside measured cell length count {}",
                self.cell_lengths.len()
            ))
        })
    }

    fn range_layout(&self, start_row: usize, row_count: usize) -> Result<layout::RowLayout> {
        self.check_range(start_row, row_count)?;
        build_fixed_width_row_range_layout(
            start_row,
            row_count,
            self.column_count,
            &self.cell_lengths,
        )
    }

    fn check_range(&self, start_row: usize, row_count: usize) -> Result<()> {
        let end_row = start_row
            .checked_add(row_count)
            .ok_or_else(|| invalid_payload("direct row range end overflowed usize"))?;
        if end_row > self.row_count {
            return Err(invalid_payload(format!(
                "direct measured row range {start_row}..{end_row} is outside measured row count {}",
                self.row_count
            )));
        }

        Ok(())
    }
}

fn measure_row_lengths(
    row_count: usize,
    column_count: usize,
    cell_lengths: &[usize],
) -> Result<(Vec<usize>, usize)> {
    let mut row_lengths = Vec::with_capacity(row_count);
    let mut payload_len = 0usize;

    for row_index in 0..row_count {
        let mut row_len = 1usize;

        for column_index in 0..column_count {
            row_len = row_len
                .checked_add(cell_lengths[row_index * column_count + column_index])
                .ok_or_else(|| invalid_payload("measured row length overflowed usize"))?;
        }

        payload_len = payload_len
            .checked_add(row_len)
            .ok_or_else(|| invalid_payload("measured payload length overflowed usize"))?;
        row_lengths.push(row_len);
    }

    Ok((row_lengths, payload_len))
}

/// Contiguous measured row range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MeasuredRowRange {
    /// First row in the measured batch.
    pub(crate) start: usize,
    /// Number of rows in this range.
    pub(crate) len: usize,
}

fn downcast_direct_array<'a, T: arrow_array::Array + 'static>(
    array: &'a dyn arrow_array::Array,
    column: &plan::DirectColumnPlan,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        value_conversion_error(row_column_diagnostic(
            column,
            0,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned direct column type",
                array.data_type()
            ),
        ))
    })
}

fn unsupported_batch(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingUnsupportedBatch,
            message,
        )]),
    }
}

fn invalid_payload(message: impl Into<String>) -> Error {
    Error::DirectEncoding {
        diagnostics: DiagnosticSet::from(vec![Diagnostic::error(
            DiagnosticCode::DirectEncodingInvalidPayload,
            message,
        )]),
    }
}

fn checked_add(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or_else(|| invalid_payload("direct encoded length overflowed usize"))
}

fn row_column_diagnostic(
    column: &plan::DirectColumnPlan,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message)
        .with_field(crate::FieldRef::new(
            column.source_index(),
            column.source_name(),
        ))
        .with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> Error {
    Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
        RecordBatch, StringArray,
    };
    use arrow_buffer::{NullBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Schema};

    use crate::{
        ArrowFieldRef, DiagnosticCode, Error, Identifier, MssqlColumn, MssqlType, MssqlTypeLength,
        SchemaMapping, conversion::arrow_to_mssql::primitive::PrimitiveArrowToMssql,
    };

    use super::plan::{DirectColumnEncoding, DirectEncoderSupport, DirectMappingSupport};
    use super::primitive::try_encode_fixed_width_primitive_rows;
    use super::{DirectEncoder, payload};

    #[test]
    fn default_direct_encoder_accepts_empty_mapping_set() {
        let encoder = DirectEncoder::new(&[]).expect("empty mapping set is supported");

        assert!(encoder.plan().is_empty());
        assert_eq!(encoder.plan().column_count(), 0);
        assert_eq!(encoder.mappings(), []);
    }

    #[test]
    fn default_direct_encoder_returns_empty_payload_for_empty_batch_and_empty_mapping_set() {
        let encoder = DirectEncoder::new(&[]).expect("empty mapping set is supported");
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));

        let payload = encoder
            .encode_batch(&batch)
            .expect("empty batch should encode as empty payload");

        assert!(payload.is_empty());
        assert_eq!(payload.row_count(), 0);
    }

    #[test]
    fn direct_encoder_reports_variable_width_column_presence() {
        let primitive = DirectEncoder::new(&[mapping(
            0,
            "quantity",
            DataType::Int32,
            MssqlType::Int,
            false,
        )])
        .unwrap();
        assert!(!primitive.has_variable_width_column());

        let mixed = DirectEncoder::new(&[
            mapping(0, "quantity", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "comment",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
        ])
        .unwrap();
        assert!(mixed.has_variable_width_column());
    }

    #[test]
    fn direct_encoder_fast_path_returns_empty_payload_for_empty_batch_with_mappings() {
        let mappings = vec![
            mapping(0, "quantity", DataType::Int32, MssqlType::Int, true),
            mapping(1, "total", DataType::Int64, MssqlType::BigInt, false),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int32Array::from(Vec::<Option<i32>>::new())) as ArrayRef,
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .unwrap();

        let payload = encoder
            .encode_batch(&batch)
            .expect("empty mapped batch should encode as empty payload");

        assert!(payload.is_empty());
        assert_eq!(payload.bytes(), []);
        assert_eq!(payload.row_token_offsets(), []);
    }

    #[test]
    fn default_direct_encoder_rejects_non_empty_row_batch_until_type_encoders_exist() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(0, "is_active".to_owned(), false, DataType::Boolean),
            MssqlColumn::new(Identifier::new("is_active").unwrap(), MssqlType::Bit, false),
        );
        let encoder =
            DirectEncoder::new_with_support(std::slice::from_ref(&mapping), &FixtureSupport)
                .expect("fixture supports the mapping");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "is_active",
                DataType::Boolean,
                false,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap();

        let payload = encoder
            .encode_batch(&batch)
            .expect("boolean is supported now");

        assert_eq!(payload.row_count(), 1);
        assert_eq!(payload.bytes(), [payload::TDS_ROW_TOKEN, 1]);
        assert_eq!(payload.row_token_offsets(), [0]);
    }

    #[test]
    fn direct_encoder_encodes_mixed_primitive_rows_in_mapping_order() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit, false),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, false),
            mapping(2, "total", DataType::Int64, MssqlType::BigInt, false),
            mapping(3, "real_value", DataType::Float32, MssqlType::Real, false),
            mapping(
                4,
                "ratio",
                DataType::Float64,
                MssqlType::Float { precision: 53 },
                false,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("is_active", DataType::Boolean, false),
                Field::new("quantity", DataType::Int32, false),
                Field::new("total", DataType::Int64, false),
                Field::new("real_value", DataType::Float32, false),
                Field::new("ratio", DataType::Float64, false),
            ],
            vec![
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, -2])),
                Arc::new(Int64Array::from(vec![10, -20])),
                Arc::new(Float32Array::from(vec![1.5, -3.25])),
                Arc::new(Float64Array::from(vec![1.25, -2.5])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 26]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                1,
                1,
                0,
                0,
                0,
                10,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0x00,
                0x00,
                0xC0,
                0x3F,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0xF4,
                0x3F,
                payload::TDS_ROW_TOKEN,
                0,
                0xFE,
                0xFF,
                0xFF,
                0xFF,
                0xEC,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0x00,
                0x00,
                0x50,
                0xC0,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x04,
                0xC0,
            ]
        );
    }

    #[test]
    fn direct_encoder_encodes_mixed_primitive_and_variable_width_rows() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "name",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Bounded(3)),
                true,
            ),
            mapping(
                2,
                "bytes",
                DataType::Binary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("bytes", DataType::Binary, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![42, -1])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("A"), None])),
                Arc::new(BinaryArray::from_iter(vec![
                    Some(&b""[..]),
                    Some(&b"xy"[..]),
                ])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 21]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                42,
                0,
                0,
                0,
                2,
                0,
                b'A',
                0,
                0xfe,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xfe,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                2,
                0,
                0,
                0,
                b'x',
                b'y',
                0,
                0,
                0,
                0,
            ]
        );
    }

    #[test]
    fn direct_encoder_row_ranges_concatenate_to_full_payload() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "name",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
            mapping(
                2,
                "bytes",
                DataType::Binary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("bytes", DataType::Binary, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    Some("東京"),
                    None,
                    Some(""),
                ])),
                Arc::new(BinaryArray::from_iter(vec![
                    Some(&b"abc"[..]),
                    None,
                    Some(&b""[..]),
                    Some(&b"\x00\xff"[..]),
                ])),
            ],
        );

        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder.encode_batch_range(&batch, 0, 2).unwrap();
        let second = encoder.encode_batch_range(&batch, 2, 2).unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(concatenated, full.bytes());
        assert_eq!(first.row_count(), 2);
        assert_eq!(second.row_count(), 2);
        assert_eq!(first.row_token_offsets()[0], 0);
        assert_eq!(second.row_token_offsets()[0], 0);
    }

    #[test]
    fn direct_encoder_measured_ranges_concatenate_to_full_payload() {
        let mappings = vec![
            mapping(0, "id", DataType::Int32, MssqlType::Int, false),
            mapping(
                1,
                "name",
                DataType::Utf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("東京")])),
            ],
        );

        let measured = encoder.measure_batch(&batch).unwrap();
        let full = encoder.encode_batch(&batch).unwrap();
        let first = encoder
            .encode_measured_batch_range(&batch, &measured, 0, 1)
            .unwrap();
        let second = encoder
            .encode_measured_batch_range(&batch, &measured, 1, 2)
            .unwrap();
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(first.bytes());
        concatenated.extend_from_slice(second.bytes());

        assert_eq!(measured.row_count(), 3);
        assert_eq!(concatenated, full.bytes());
        assert_eq!(first.row_token_offsets()[0], 0);
        assert_eq!(second.row_token_offsets()[0], 0);
    }

    #[test]
    fn measured_direct_batch_ranges_split_by_payload_byte_limit() {
        let mappings = vec![mapping(0, "id", DataType::Int32, MssqlType::Int, false)];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("id", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
        );

        let measured = encoder.measure_batch(&batch).unwrap();

        assert_eq!(measured.payload_len(), 20);
        assert_eq!(
            measured.row_ranges(10).unwrap(),
            [
                super::MeasuredRowRange { start: 0, len: 2 },
                super::MeasuredRowRange { start: 2, len: 2 },
            ]
        );
        assert_eq!(
            measured.row_ranges(4).unwrap(),
            [
                super::MeasuredRowRange { start: 0, len: 1 },
                super::MeasuredRowRange { start: 1, len: 1 },
                super::MeasuredRowRange { start: 2, len: 1 },
                super::MeasuredRowRange { start: 3, len: 1 },
            ]
        );
    }

    #[test]
    fn direct_encoder_row_range_rejects_out_of_bounds_range() {
        let mappings = vec![mapping(0, "id", DataType::Int32, MssqlType::Int, false)];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("id", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        );

        let err = encoder
            .encode_batch_range(&batch, 1, 2)
            .expect_err("range past batch end must fail");

        assert_direct_encoding_diagnostic(err, DiagnosticCode::DirectEncodingInvalidPayload);
    }

    #[test]
    fn direct_encoder_encodes_nullable_primitive_cells() {
        let mappings = vec![
            mapping(0, "is_active", DataType::Boolean, MssqlType::Bit, true),
            mapping(1, "quantity", DataType::Int32, MssqlType::Int, true),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("is_active", DataType::Boolean, true),
                Field::new("quantity", DataType::Int32, true),
            ],
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![None, Some(7)])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 4]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                1,
                1,
                0,
                payload::TDS_ROW_TOKEN,
                0,
                4,
                7,
                0,
                0,
                0
            ]
        );
    }

    #[test]
    fn direct_encoder_fast_path_encodes_mixed_nullable_and_non_nullable_rows() {
        let mappings = vec![
            mapping(0, "quantity", DataType::Int32, MssqlType::Int, true),
            mapping(1, "total", DataType::Int64, MssqlType::BigInt, false),
            mapping(2, "active", DataType::Boolean, MssqlType::Bit, true),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, false),
                Field::new("active", DataType::Boolean, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![Some(10), None, Some(-1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100, 200, 300])),
                Arc::new(BooleanArray::from(vec![None, Some(true), Some(false)])),
            ],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 15, 27]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                4,
                10,
                0,
                0,
                0,
                100,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                payload::TDS_ROW_TOKEN,
                0,
                200,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                1,
                payload::TDS_ROW_TOKEN,
                4,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0x2C,
                0x01,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0
            ]
        );
    }

    #[test]
    fn direct_encoder_fixed_width_fast_path_is_active_for_mixed_nullability() {
        let mappings = vec![
            mapping(0, "quantity", DataType::Int32, MssqlType::Int, true),
            mapping(1, "total", DataType::Int64, MssqlType::BigInt, false),
            mapping(
                2,
                "ratio",
                DataType::Float64,
                MssqlType::Float { precision: 53 },
                true,
            ),
        ];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, false),
                Field::new("ratio", DataType::Float64, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![Some(10), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Float64Array::from(vec![None, Some(1.5)])),
            ],
        );

        let payload = try_encode_fixed_width_primitive_rows(&batch, encoder.plan().columns())
            .unwrap()
            .expect("fixed-width primitive fast path should be active");

        assert_eq!(payload.row_token_offsets(), [0, 15]);
        assert_eq!(payload.row_count(), 2);
    }

    #[test]
    fn direct_encoder_fast_path_does_not_read_non_finite_float_from_null_slot() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Float64Array::new(
            ScalarBuffer::from(vec![f64::NAN, 1.5]),
            Some(NullBuffer::from(vec![false, true])),
        );
        let batch = record_batch(
            vec![Field::new("ratio", DataType::Float64, true)],
            vec![Arc::new(array)],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 2]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                0,
                payload::TDS_ROW_TOKEN,
                8,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0xF8,
                0x3F
            ]
        );
    }

    #[test]
    fn direct_encoder_fast_path_does_not_read_non_finite_float32_from_null_slot() {
        let mappings = vec![mapping(
            0,
            "real_value",
            DataType::Float32,
            MssqlType::Real,
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let array = Float32Array::new(
            ScalarBuffer::from(vec![f32::NAN, 1.5]),
            Some(NullBuffer::from(vec![false, true])),
        );
        let batch = record_batch(
            vec![Field::new("real_value", DataType::Float32, true)],
            vec![Arc::new(array)],
        );

        let payload = encoder.encode_batch(&batch).unwrap();

        assert_eq!(payload.row_token_offsets(), [0, 2]);
        assert_eq!(
            payload.bytes(),
            [
                payload::TDS_ROW_TOKEN,
                0,
                payload::TDS_ROW_TOKEN,
                4,
                0x00,
                0x00,
                0xC0,
                0x3F
            ]
        );
    }

    #[test]
    fn direct_encoder_fast_path_rejects_non_finite_nullable_float_when_non_null() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            true,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("ratio", DataType::Float64, true)],
            vec![Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(f64::NAN),
            ]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("non-null non-finite float must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(1),
            Some((0, "ratio")),
        );
    }

    #[test]
    fn direct_encoder_fast_path_rejects_null_in_non_nullable_column() {
        let mappings = vec![mapping(
            0,
            "quantity",
            DataType::Int32,
            MssqlType::Int,
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("quantity", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("null in non-nullable direct column must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(1),
            Some((0, "quantity")),
        );
    }

    #[test]
    fn direct_encoder_rejects_non_finite_float_before_returning_payload() {
        let mappings = vec![mapping(
            0,
            "ratio",
            DataType::Float64,
            MssqlType::Float { precision: 53 },
            false,
        )];
        let encoder = DirectEncoder::new(&mappings).unwrap();
        let batch = record_batch(
            vec![Field::new("ratio", DataType::Float64, false)],
            vec![Arc::new(Float64Array::from(vec![1.0, f64::NAN]))],
        );

        let err = encoder
            .encode_batch(&batch)
            .expect_err("non-finite float must fail");

        assert_value_conversion_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(1),
            Some((0, "ratio")),
        );
    }

    #[derive(Debug, Clone, Copy)]
    struct FixtureSupport;

    impl DirectEncoderSupport for FixtureSupport {
        fn support_mapping(&self, mapping: &SchemaMapping) -> DirectMappingSupport {
            DirectMappingSupport::Supported {
                encoding: DirectColumnEncoding::Primitive(
                    PrimitiveArrowToMssql::classify(mapping, 0).unwrap(),
                ),
            }
        }
    }

    fn assert_unsupported_batch(err: Error) {
        assert_direct_encoding_diagnostic(err, DiagnosticCode::DirectEncodingUnsupportedBatch);
    }

    fn assert_direct_encoding_diagnostic(err: Error, expected_code: DiagnosticCode) {
        let Error::DirectEncoding { diagnostics } = err else {
            panic!("expected direct encoding error");
        };

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics.all()[0].code(), expected_code);
    }

    fn mapping(
        index: usize,
        name: &str,
        arrow_type: DataType,
        mssql_type: MssqlType,
        nullable: bool,
    ) -> SchemaMapping {
        SchemaMapping::new(
            ArrowFieldRef::new(index, name.to_owned(), nullable, arrow_type),
            MssqlColumn::new(Identifier::new(name).unwrap(), mssql_type, nullable),
        )
    }

    fn record_batch(fields: Vec<Field>, arrays: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    fn assert_value_conversion_diagnostic(
        err: Error,
        expected_code: DiagnosticCode,
        expected_row: Option<usize>,
        expected_field: Option<(usize, &str)>,
    ) {
        let Error::ValueConversion { diagnostics } = err else {
            panic!("expected value conversion error");
        };

        assert_eq!(diagnostics.len(), 1);
        let diagnostic = &diagnostics.all()[0];
        assert_eq!(diagnostic.code(), expected_code);
        assert_eq!(diagnostic.row(), expected_row);
        assert_eq!(
            diagnostic
                .field()
                .map(|field| (field.index(), field.name())),
            expected_field
        );
    }
}
