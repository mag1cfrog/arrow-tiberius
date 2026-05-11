//! Runtime record batch conversion scaffolding.

#![allow(dead_code)]

use std::borrow::Cow;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;

use crate::{
    Diagnostic, DiagnosticCode, DiagnosticSet, FieldRef, MssqlType, MssqlTypeLength, Result,
    SchemaMapping,
};

/// Borrowed value extracted from one Arrow array cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ArrowCell<'a> {
    /// Arrow null value.
    Null,
    /// Arrow boolean value.
    Boolean(bool),
    /// Arrow signed 8-bit integer value.
    Int8(i8),
    /// Arrow signed 16-bit integer value.
    Int16(i16),
    /// Arrow signed 32-bit integer value.
    Int32(i32),
    /// Arrow signed 64-bit integer value.
    Int64(i64),
    /// Arrow unsigned 8-bit integer value.
    UInt8(u8),
    /// Arrow unsigned 16-bit integer value.
    UInt16(u16),
    /// Arrow unsigned 32-bit integer value.
    UInt32(u32),
    /// Arrow unsigned 64-bit integer value.
    UInt64(u64),
    /// Arrow 32-bit floating point value.
    Float32(f32),
    /// Arrow 64-bit floating point value.
    Float64(f64),
    /// Arrow UTF-8 string value.
    Utf8(&'a str),
    /// Arrow binary value.
    Binary(&'a [u8]),
}

impl<'a> ArrowCell<'a> {
    fn try_bool(self, mapping: &SchemaMapping, row_index: usize) -> Result<bool> {
        match self {
            Self::Boolean(value) => Ok(value),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow boolean payload, got {other:?}"),
            ))),
        }
    }

    fn try_u8(self, mapping: &SchemaMapping, row_index: usize) -> Result<u8> {
        match self {
            Self::UInt8(value) => Ok(value),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow UInt8 payload, got {other:?}"),
            ))),
        }
    }

    fn try_i16(self, mapping: &SchemaMapping, row_index: usize) -> Result<i16> {
        match self {
            Self::Int8(value) => Ok(i16::from(value)),
            Self::Int16(value) => Ok(value),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow Int8 or Int16 payload, got {other:?}"),
            ))),
        }
    }

    fn try_i32(self, mapping: &SchemaMapping, row_index: usize) -> Result<i32> {
        match self {
            Self::Int32(value) => Ok(value),
            Self::UInt16(value) => Ok(i32::from(value)),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow Int32 or UInt16 payload, got {other:?}"),
            ))),
        }
    }

    fn try_i64(self, mapping: &SchemaMapping, row_index: usize) -> Result<i64> {
        match self {
            Self::Int64(value) => Ok(value),
            Self::UInt32(value) => Ok(i64::from(value)),
            Self::UInt64(value) => i64::try_from(value).map_err(|_| {
                value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::IntegerOutOfRange,
                    format!("Arrow UInt64 value {value} does not fit planned SQL Server bigint"),
                ))
            }),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow Int64, UInt32, or UInt64 payload, got {other:?}"),
            ))),
        }
    }

    fn try_decimal(self, mapping: &SchemaMapping, row_index: usize) -> Result<MssqlDecimal> {
        if !is_uint64_decimal20_0_mapping(mapping) {
            return Err(unsupported_value_conversion(
                mapping,
                row_index,
                format!(
                    "planned SQL Server type {} is not supported yet",
                    mapping.mssql().ty().to_sql()
                ),
            ));
        }

        match self {
            Self::UInt64(value) => Ok(MssqlDecimal::new(i128::from(value), 0)),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow UInt64 payload, got {other:?}"),
            ))),
        }
    }

    fn try_f32(self, mapping: &SchemaMapping, row_index: usize) -> Result<f32> {
        match self {
            Self::Float32(value) if value.is_finite() => Ok(value),
            Self::Float32(value) => Err(non_finite_float_error(mapping, row_index, value)),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow Float32 payload, got {other:?}"),
            ))),
        }
    }

    fn try_f64(self, mapping: &SchemaMapping, row_index: usize) -> Result<f64> {
        match self {
            Self::Float64(value) if value.is_finite() => Ok(value),
            Self::Float64(value) => Err(non_finite_float_error(mapping, row_index, value)),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow Float64 payload, got {other:?}"),
            ))),
        }
    }

    fn try_str(self, mapping: &SchemaMapping, row_index: usize) -> Result<&'a str> {
        match self {
            Self::Utf8(value) => Ok(value),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow UTF-8 payload, got {other:?}"),
            ))),
        }
    }

    fn try_bytes(self, mapping: &SchemaMapping, row_index: usize) -> Result<&'a [u8]> {
        match self {
            Self::Binary(value) => Ok(value),
            other => Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow binary payload, got {other:?}"),
            ))),
        }
    }
}

/// Semantic SQL Server value for one planned cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum MssqlCell<'a> {
    /// SQL Server `bit` cell.
    Bit(Option<bool>),
    /// SQL Server `tinyint` cell.
    TinyInt(Option<u8>),
    /// SQL Server `smallint` cell.
    SmallInt(Option<i16>),
    /// SQL Server `int` cell.
    Int(Option<i32>),
    /// SQL Server `bigint` cell.
    BigInt(Option<i64>),
    /// SQL Server `decimal` cell.
    Decimal(Option<MssqlDecimal>),
    /// SQL Server `real` cell.
    Real(Option<f32>),
    /// SQL Server `float` cell.
    Float(Option<f64>),
    /// SQL Server `nvarchar` cell.
    NVarChar(Option<&'a str>),
    /// SQL Server `varbinary` cell.
    VarBinary(Option<&'a [u8]>),
}

/// Semantic SQL Server decimal value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MssqlDecimal {
    unscaled: i128,
    scale: u8,
}

impl MssqlDecimal {
    /// Creates a semantic decimal value from its unscaled integer and scale.
    const fn new(unscaled: i128, scale: u8) -> Self {
        Self { unscaled, scale }
    }

    /// Returns the unscaled integer value.
    pub(crate) const fn unscaled(self) -> i128 {
        self.unscaled
    }

    /// Returns the decimal scale.
    pub(crate) const fn scale(self) -> u8 {
        self.scale
    }

    fn to_tiberius_numeric(self) -> tiberius::numeric::Numeric {
        tiberius::numeric::Numeric::new_with_scale(self.unscaled, self.scale)
    }
}

/// Borrowed conversion view over one Arrow record batch and schema mappings.
#[derive(Debug)]
pub(crate) struct RecordBatchView<'a> {
    batch: &'a RecordBatch,
    mappings: &'a [SchemaMapping],
}

impl<'a> RecordBatchView<'a> {
    /// Creates a conversion view after validating batch columns against mappings.
    pub(crate) fn new(batch: &'a RecordBatch, mappings: &'a [SchemaMapping]) -> Result<Self> {
        validate_runtime_columns(batch, mappings)?;

        Ok(Self { batch, mappings })
    }

    /// Returns the number of rows in the runtime batch.
    pub(crate) fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns the planned mappings in conversion order.
    pub(crate) const fn mappings(&self) -> &[SchemaMapping] {
        self.mappings
    }

    /// Checks that a row index is inside the runtime batch.
    pub(crate) fn check_row_index(&self, row_index: usize) -> Result<()> {
        if row_index < self.row_count() {
            return Ok(());
        }

        let message = format!(
            "row index {row_index} is outside runtime batch with {} row(s)",
            self.row_count()
        );
        Err(value_conversion_error(
            Diagnostic::error(DiagnosticCode::RowIndexOutOfBounds, message).with_row(row_index),
        ))
    }

    /// Extracts one borrowed Arrow cell from a planned mapping and row index.
    fn arrow_cell(&self, mapping: &SchemaMapping, row_index: usize) -> Result<ArrowCell<'_>> {
        self.check_row_index(row_index)?;

        let Some(array) = self
            .batch
            .columns()
            .get(mapping.arrow().index())
            .map(AsRef::as_ref)
        else {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                "planned column index is outside the runtime batch",
            )));
        };

        extract_arrow_cell(array, mapping, row_index)
    }

    /// Converts one planned cell into a semantic SQL Server cell.
    fn mssql_cell(&self, mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'_>> {
        let cell = self.arrow_cell(mapping, row_index)?;
        mssql_cell_from_arrow_cell(mapping, cell, row_index)
    }

    /// Converts one runtime row into semantic SQL Server cells in mapping order.
    pub(crate) fn mssql_row(&self, row_index: usize) -> Result<Vec<MssqlCell<'_>>> {
        self.check_row_index(row_index)?;

        let mut cells = Vec::with_capacity(self.mappings.len());
        for mapping in self.mappings {
            cells.push(self.mssql_cell(mapping, row_index)?);
        }

        Ok(cells)
    }

    /// Converts one runtime row into an owned Tiberius token row.
    pub(crate) fn tiberius_row_owned(
        &self,
        row_index: usize,
    ) -> Result<tiberius::TokenRow<'static>> {
        let cells = self.mssql_row(row_index)?;
        let mut row = tiberius::TokenRow::with_capacity(cells.len());

        for cell in cells {
            row.push(mssql_cell_to_tiberius_owned(cell));
        }

        Ok(row)
    }
}

/// Converts a semantic SQL Server cell into borrowed Tiberius column data.
pub(crate) fn mssql_cell_to_tiberius_borrowed(cell: MssqlCell<'_>) -> tiberius::ColumnData<'_> {
    match cell {
        MssqlCell::Bit(value) => tiberius::ColumnData::Bit(value),
        MssqlCell::TinyInt(value) => tiberius::ColumnData::U8(value),
        MssqlCell::SmallInt(value) => tiberius::ColumnData::I16(value),
        MssqlCell::Int(value) => tiberius::ColumnData::I32(value),
        MssqlCell::BigInt(value) => tiberius::ColumnData::I64(value),
        MssqlCell::Decimal(value) => {
            tiberius::ColumnData::Numeric(value.map(MssqlDecimal::to_tiberius_numeric))
        }
        MssqlCell::Real(value) => tiberius::ColumnData::F32(value),
        MssqlCell::Float(value) => tiberius::ColumnData::F64(value),
        MssqlCell::NVarChar(value) => tiberius::ColumnData::String(value.map(Cow::Borrowed)),
        MssqlCell::VarBinary(value) => tiberius::ColumnData::Binary(value.map(Cow::Borrowed)),
    }
}

/// Converts a semantic SQL Server cell into owned Tiberius column data.
pub(crate) fn mssql_cell_to_tiberius_owned(cell: MssqlCell<'_>) -> tiberius::ColumnData<'static> {
    match cell {
        MssqlCell::Bit(value) => tiberius::ColumnData::Bit(value),
        MssqlCell::TinyInt(value) => tiberius::ColumnData::U8(value),
        MssqlCell::SmallInt(value) => tiberius::ColumnData::I16(value),
        MssqlCell::Int(value) => tiberius::ColumnData::I32(value),
        MssqlCell::BigInt(value) => tiberius::ColumnData::I64(value),
        MssqlCell::Decimal(value) => {
            tiberius::ColumnData::Numeric(value.map(MssqlDecimal::to_tiberius_numeric))
        }
        MssqlCell::Real(value) => tiberius::ColumnData::F32(value),
        MssqlCell::Float(value) => tiberius::ColumnData::F64(value),
        MssqlCell::NVarChar(value) => {
            tiberius::ColumnData::String(value.map(|value| Cow::Owned(value.to_owned())))
        }
        MssqlCell::VarBinary(value) => {
            tiberius::ColumnData::Binary(value.map(|value| Cow::Owned(value.to_vec())))
        }
    }
}

fn extract_arrow_cell<'a>(
    array: &'a dyn Array,
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<ArrowCell<'a>> {
    if array.is_null(row_index) {
        return Ok(ArrowCell::Null);
    }

    match mapping.arrow().data_type() {
        DataType::Boolean => {
            let array = downcast_array::<BooleanArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Boolean(array.value(row_index)))
        }
        DataType::Int8 => {
            let array = downcast_array::<Int8Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int8(array.value(row_index)))
        }
        DataType::Int16 => {
            let array = downcast_array::<Int16Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int16(array.value(row_index)))
        }
        DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int32(array.value(row_index)))
        }
        DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Int64(array.value(row_index)))
        }
        DataType::UInt8 => {
            let array = downcast_array::<UInt8Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt8(array.value(row_index)))
        }
        DataType::UInt16 => {
            let array = downcast_array::<UInt16Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt16(array.value(row_index)))
        }
        DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt32(array.value(row_index)))
        }
        DataType::UInt64 => {
            let array = downcast_array::<UInt64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::UInt64(array.value(row_index)))
        }
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float32(array.value(row_index)))
        }
        DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array, mapping, row_index)?;
            Ok(ArrowCell::Float64(array.value(row_index)))
        }
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        DataType::LargeUtf8 => {
            let array = downcast_array::<LargeStringArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Utf8(array.value(row_index)))
        }
        DataType::Binary => {
            let array = downcast_array::<BinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        DataType::LargeBinary => {
            let array = downcast_array::<LargeBinaryArray>(array, mapping, row_index)?;
            Ok(ArrowCell::Binary(array.value(row_index)))
        }
        other => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!("Arrow value extraction for {other} is not supported yet"),
        )),
    }
}

fn mssql_cell_from_arrow_cell<'a>(
    mapping: &SchemaMapping,
    cell: ArrowCell<'a>,
    row_index: usize,
) -> Result<MssqlCell<'a>> {
    if matches!(cell, ArrowCell::Null) {
        if !mapping.mssql().nullable() {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::NullInNonNullableColumn,
                "null value in non-nullable planned column",
            )));
        }

        return null_mssql_cell(mapping, row_index);
    }

    match mapping.mssql().ty() {
        MssqlType::Bit => Ok(MssqlCell::Bit(Some(cell.try_bool(mapping, row_index)?))),
        MssqlType::TinyInt => Ok(MssqlCell::TinyInt(Some(cell.try_u8(mapping, row_index)?))),
        MssqlType::SmallInt => Ok(MssqlCell::SmallInt(Some(cell.try_i16(mapping, row_index)?))),
        MssqlType::Int => Ok(MssqlCell::Int(Some(cell.try_i32(mapping, row_index)?))),
        MssqlType::BigInt => Ok(MssqlCell::BigInt(Some(cell.try_i64(mapping, row_index)?))),
        MssqlType::Decimal { .. } => Ok(MssqlCell::Decimal(Some(
            cell.try_decimal(mapping, row_index)?,
        ))),
        MssqlType::Real => Ok(MssqlCell::Real(Some(cell.try_f32(mapping, row_index)?))),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(Some(cell.try_f64(mapping, row_index)?))),
        MssqlType::NVarChar(length) => nvar_char_cell(mapping, row_index, *length, cell),
        MssqlType::VarBinary(length) => var_binary_cell(mapping, row_index, *length, cell),
        ty => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "planned SQL Server type {} is not supported yet",
                ty.to_sql()
            ),
        )),
    }
}

fn null_mssql_cell<'a>(mapping: &SchemaMapping, row_index: usize) -> Result<MssqlCell<'a>> {
    match mapping.mssql().ty() {
        MssqlType::Bit => Ok(MssqlCell::Bit(None)),
        MssqlType::TinyInt => Ok(MssqlCell::TinyInt(None)),
        MssqlType::SmallInt => Ok(MssqlCell::SmallInt(None)),
        MssqlType::Int => Ok(MssqlCell::Int(None)),
        MssqlType::BigInt => Ok(MssqlCell::BigInt(None)),
        MssqlType::Decimal { .. } if is_uint64_decimal20_0_mapping(mapping) => {
            Ok(MssqlCell::Decimal(None))
        }
        MssqlType::Real => Ok(MssqlCell::Real(None)),
        MssqlType::Float { .. } => Ok(MssqlCell::Float(None)),
        MssqlType::NVarChar(_) => Ok(MssqlCell::NVarChar(None)),
        MssqlType::VarBinary(_) => Ok(MssqlCell::VarBinary(None)),
        ty => Err(unsupported_value_conversion(
            mapping,
            row_index,
            format!(
                "planned SQL Server type {} is not supported yet",
                ty.to_sql()
            ),
        )),
    }
}

fn is_uint64_decimal20_0_mapping(mapping: &SchemaMapping) -> bool {
    matches!(
        (mapping.arrow().data_type(), mapping.mssql().ty()),
        (
            DataType::UInt64,
            MssqlType::Decimal {
                precision: 20,
                scale: 0
            }
        )
    )
}

fn nvar_char_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    length: MssqlTypeLength,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    let value = cell.try_str(mapping, row_index)?;
    let code_units = value.encode_utf16().count();

    if exceeds_length(length, code_units) {
        return Err(value_too_long_error(
            mapping,
            row_index,
            format!(
                "string value has {code_units} UTF-16 code unit(s), exceeding planned {}",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    Ok(MssqlCell::NVarChar(Some(value)))
}

fn var_binary_cell<'a>(
    mapping: &SchemaMapping,
    row_index: usize,
    length: MssqlTypeLength,
    cell: ArrowCell<'a>,
) -> Result<MssqlCell<'a>> {
    let value = cell.try_bytes(mapping, row_index)?;
    let bytes = value.len();

    if exceeds_length(length, bytes) {
        return Err(value_too_long_error(
            mapping,
            row_index,
            format!(
                "binary value has {bytes} byte(s), exceeding planned {}",
                mapping.mssql().ty().to_sql()
            ),
        ));
    }

    Ok(MssqlCell::VarBinary(Some(value)))
}

fn exceeds_length(length: MssqlTypeLength, actual: usize) -> bool {
    match length {
        MssqlTypeLength::Bounded(limit) => actual > limit,
        MssqlTypeLength::Max => false,
    }
}

fn downcast_array<'a, T: Array + 'static>(
    array: &'a dyn Array,
    mapping: &SchemaMapping,
    row_index: usize,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                array.data_type(),
                mapping.arrow().data_type()
            ),
        ))
    })
}

fn unsupported_value_conversion(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueConversionUnsupported,
        message,
    ))
}

fn non_finite_float_error(
    mapping: &SchemaMapping,
    row_index: usize,
    value: impl std::fmt::Display,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::NonFiniteFloat,
        format!("non-finite floating point value {value} is not supported"),
    ))
}

fn value_too_long_error(
    mapping: &SchemaMapping,
    row_index: usize,
    message: impl Into<String>,
) -> crate::Error {
    value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::ValueTooLong,
        message,
    ))
}

fn validate_runtime_columns(batch: &RecordBatch, mappings: &[SchemaMapping]) -> Result<()> {
    if batch.num_columns() < mappings.len() {
        let mapping = &mappings[batch.num_columns()];
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::SchemaMismatch,
            format!(
                "planned column index {} is outside runtime batch with {} column(s)",
                mapping.arrow().index(),
                batch.num_columns()
            ),
        )));
    }

    if batch.num_columns() > mappings.len() {
        return Err(value_conversion_error(Diagnostic::error(
            DiagnosticCode::SchemaMismatch,
            format!(
                "runtime batch has {} column(s) but mappings contain {} column(s)",
                batch.num_columns(),
                mappings.len()
            ),
        )));
    }

    for (position, (field, (array, mapping))) in batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns().iter().zip(mappings))
        .enumerate()
    {
        if mapping.arrow().index() != position {
            return Err(value_conversion_error(mapping_diagnostic(
                mapping,
                DiagnosticCode::SchemaMismatch,
                format!(
                    "mapping position {position} does not match planned Arrow field index {}",
                    mapping.arrow().index()
                ),
            )));
        }

        if field.name() != mapping.arrow().name() {
            return Err(value_conversion_error(mapping_diagnostic(
                mapping,
                DiagnosticCode::SchemaMismatch,
                format!(
                    "runtime Arrow field name {} does not match planned Arrow field name {}",
                    field.name(),
                    mapping.arrow().name()
                ),
            )));
        }

        validate_runtime_column(array.as_ref(), mapping)?;
    }

    Ok(())
}

fn validate_runtime_column(array: &dyn Array, mapping: &SchemaMapping) -> Result<()> {
    if array.data_type() != mapping.arrow().data_type() {
        return Err(value_conversion_error(mapping_diagnostic(
            mapping,
            DiagnosticCode::SchemaMismatch,
            format!(
                "runtime Arrow type {} does not match planned Arrow type {}",
                array.data_type(),
                mapping.arrow().data_type()
            ),
        )));
    }

    Ok(())
}

fn mapping_diagnostic(
    mapping: &SchemaMapping,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(code, message).with_field(FieldRef::new(
        mapping.arrow().index(),
        mapping.arrow().name(),
    ))
}

fn row_mapping_diagnostic(
    mapping: &SchemaMapping,
    row_index: usize,
    code: DiagnosticCode,
    message: impl Into<String>,
) -> Diagnostic {
    mapping_diagnostic(mapping, code, message).with_row(row_index)
}

fn value_conversion_error(diagnostic: Diagnostic) -> crate::Error {
    crate::Error::ValueConversion {
        diagnostics: DiagnosticSet::from(vec![diagnostic]),
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array, new_null_array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{
        ArrowCell, MssqlCell, MssqlDecimal, RecordBatchView, mssql_cell_to_tiberius_borrowed,
        mssql_cell_to_tiberius_owned,
    };
    use crate::{
        ArrowFieldRef, BinaryPolicy, Date64Policy, DiagnosticCode, Error, Identifier, MssqlColumn,
        MssqlProfile, MssqlType, PlanOptions, SchemaMapping, StringPolicy, UInt64Policy,
        plan_arrow_schema_to_mssql_mappings,
    };

    #[test]
    fn accepts_matching_batch_and_mappings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), None])),
            ],
        )
        .unwrap();

        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(view.row_count(), 2);
        assert_eq!(view.mappings().len(), 2);
        view.check_row_index(1).unwrap();
    }

    #[test]
    fn extracts_arrow_cells_for_supported_initial_primitives() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("total", DataType::Int64, true),
            Field::new("unsigned_tiny", DataType::UInt8, true),
            Field::new("unsigned_medium", DataType::UInt16, true),
            Field::new("unsigned_large", DataType::UInt32, true),
            Field::new("real_value", DataType::Float32, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("active", DataType::Boolean, true),
                Field::new("tiny", DataType::Int8, true),
                Field::new("small", DataType::Int16, true),
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, true),
                Field::new("unsigned_tiny", DataType::UInt8, true),
                Field::new("unsigned_medium", DataType::UInt16, true),
                Field::new("unsigned_large", DataType::UInt32, true),
                Field::new("real_value", DataType::Float32, true),
                Field::new("float_value", DataType::Float64, true),
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int8Array::from(vec![Some(-8_i8), None])),
                Arc::new(Int16Array::from(vec![Some(-16_i16), None])),
                Arc::new(Int32Array::from(vec![Some(12_i32), None])),
                Arc::new(Int64Array::from(vec![Some(34_i64), None])),
                Arc::new(UInt8Array::from(vec![Some(8_u8), None])),
                Arc::new(UInt16Array::from(vec![Some(16_u16), None])),
                Arc::new(UInt32Array::from(vec![Some(32_u32), None])),
                Arc::new(Float32Array::from(vec![Some(1.25_f32), None])),
                Arc::new(Float64Array::from(vec![Some(2.5_f64), None])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(LargeStringArray::from(vec![Some("東京"), None])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
                Arc::new(LargeBinaryArray::from(vec![Some(&b"large"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::Boolean(true)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[1], 0).unwrap(),
            ArrowCell::Int8(-8)
        );
        assert_eq!(view.arrow_cell(&mappings[1], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[2], 0).unwrap(),
            ArrowCell::Int16(-16)
        );
        assert_eq!(view.arrow_cell(&mappings[2], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[3], 0).unwrap(),
            ArrowCell::Int32(12)
        );
        assert_eq!(view.arrow_cell(&mappings[3], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[4], 0).unwrap(),
            ArrowCell::Int64(34)
        );
        assert_eq!(view.arrow_cell(&mappings[4], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[5], 0).unwrap(),
            ArrowCell::UInt8(8)
        );
        assert_eq!(view.arrow_cell(&mappings[5], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[6], 0).unwrap(),
            ArrowCell::UInt16(16)
        );
        assert_eq!(view.arrow_cell(&mappings[6], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[7], 0).unwrap(),
            ArrowCell::UInt32(32)
        );
        assert_eq!(view.arrow_cell(&mappings[7], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[8], 0).unwrap(),
            ArrowCell::Float32(1.25)
        );
        assert_eq!(view.arrow_cell(&mappings[8], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[9], 0).unwrap(),
            ArrowCell::Float64(2.5)
        );
        assert_eq!(view.arrow_cell(&mappings[9], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[10], 0).unwrap(),
            ArrowCell::Utf8("hello")
        );
        assert_eq!(view.arrow_cell(&mappings[10], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[11], 0).unwrap(),
            ArrowCell::Utf8("東京")
        );
        assert_eq!(view.arrow_cell(&mappings[11], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[12], 0).unwrap(),
            ArrowCell::Binary(b"abc")
        );
        assert_eq!(view.arrow_cell(&mappings[12], 1).unwrap(), ArrowCell::Null);
        assert_eq!(
            view.arrow_cell(&mappings[13], 0).unwrap(),
            ArrowCell::Binary(b"large")
        );
        assert_eq!(view.arrow_cell(&mappings[13], 1).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn extracts_uint64_arrow_cells_at_policy_boundaries() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("unsigned_huge", DataType::UInt64, true)]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_huge",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                Some((i64::MAX as u64) + 1),
                Some(u64::MAX),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.arrow_cell(&mappings[0], 0).unwrap(),
            ArrowCell::UInt64(0)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 1).unwrap(),
            ArrowCell::UInt64(i64::MAX as u64)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 2).unwrap(),
            ArrowCell::UInt64((i64::MAX as u64) + 1)
        );
        assert_eq!(
            view.arrow_cell(&mappings[0], 3).unwrap(),
            ArrowCell::UInt64(u64::MAX)
        );
        assert_eq!(view.arrow_cell(&mappings[0], 4).unwrap(), ArrowCell::Null);
    }

    #[test]
    fn converts_supported_initial_primitives_to_mssql_cells() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("total", DataType::Int64, true),
            Field::new("unsigned_tiny", DataType::UInt8, true),
            Field::new("unsigned_medium", DataType::UInt16, true),
            Field::new("unsigned_large", DataType::UInt32, true),
            Field::new("real_value", DataType::Float32, true),
            Field::new("float_value", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("active", DataType::Boolean, true),
                Field::new("tiny", DataType::Int8, true),
                Field::new("small", DataType::Int16, true),
                Field::new("quantity", DataType::Int32, true),
                Field::new("total", DataType::Int64, true),
                Field::new("unsigned_tiny", DataType::UInt8, true),
                Field::new("unsigned_medium", DataType::UInt16, true),
                Field::new("unsigned_large", DataType::UInt32, true),
                Field::new("real_value", DataType::Float32, true),
                Field::new("float_value", DataType::Float64, true),
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                Arc::new(Int8Array::from(vec![Some(-8_i8), None])),
                Arc::new(Int16Array::from(vec![Some(-16_i16), None])),
                Arc::new(Int32Array::from(vec![Some(12_i32), None])),
                Arc::new(Int64Array::from(vec![Some(34_i64), None])),
                Arc::new(UInt8Array::from(vec![Some(8_u8), None])),
                Arc::new(UInt16Array::from(vec![Some(16_u16), None])),
                Arc::new(UInt32Array::from(vec![Some(32_u32), None])),
                Arc::new(Float32Array::from(vec![Some(1.25_f32), None])),
                Arc::new(Float64Array::from(vec![Some(2.5_f64), None])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(LargeStringArray::from(vec![Some("東京"), None])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
                Arc::new(LargeBinaryArray::from(vec![Some(&b"large"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Bit(Some(true))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Bit(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::SmallInt(Some(-8))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::SmallInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::SmallInt(Some(-16))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::SmallInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 0).unwrap(),
            MssqlCell::Int(Some(12))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 1).unwrap(),
            MssqlCell::Int(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 0).unwrap(),
            MssqlCell::BigInt(Some(34))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 1).unwrap(),
            MssqlCell::BigInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[5], 0).unwrap(),
            MssqlCell::TinyInt(Some(8))
        );
        assert_eq!(
            view.mssql_cell(&mappings[5], 1).unwrap(),
            MssqlCell::TinyInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[6], 0).unwrap(),
            MssqlCell::Int(Some(16))
        );
        assert_eq!(
            view.mssql_cell(&mappings[6], 1).unwrap(),
            MssqlCell::Int(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[7], 0).unwrap(),
            MssqlCell::BigInt(Some(32))
        );
        assert_eq!(
            view.mssql_cell(&mappings[7], 1).unwrap(),
            MssqlCell::BigInt(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[8], 0).unwrap(),
            MssqlCell::Real(Some(1.25))
        );
        assert_eq!(
            view.mssql_cell(&mappings[8], 1).unwrap(),
            MssqlCell::Real(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[9], 0).unwrap(),
            MssqlCell::Float(Some(2.5))
        );
        assert_eq!(
            view.mssql_cell(&mappings[9], 1).unwrap(),
            MssqlCell::Float(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[10], 0).unwrap(),
            MssqlCell::NVarChar(Some("hello"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[10], 1).unwrap(),
            MssqlCell::NVarChar(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[11], 0).unwrap(),
            MssqlCell::NVarChar(Some("東京"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[11], 1).unwrap(),
            MssqlCell::NVarChar(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[12], 0).unwrap(),
            MssqlCell::VarBinary(Some(b"abc"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[12], 1).unwrap(),
            MssqlCell::VarBinary(None)
        );
        assert_eq!(
            view.mssql_cell(&mappings[13], 0).unwrap(),
            MssqlCell::VarBinary(Some(b"large"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[13], 1).unwrap(),
            MssqlCell::VarBinary(None)
        );
    }

    #[test]
    fn converts_runtime_row_to_mssql_cells_in_mapping_order() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("payload", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32, 2])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let first_row = view.mssql_row(0).unwrap();
        assert_eq!(
            first_row,
            vec![
                MssqlCell::Int(Some(1)),
                MssqlCell::Bit(Some(true)),
                MssqlCell::NVarChar(Some("first")),
                MssqlCell::VarBinary(Some(b"abc")),
            ]
        );

        let second_row = view.mssql_row(1).unwrap();
        assert_eq!(
            second_row,
            vec![
                MssqlCell::Int(Some(2)),
                MssqlCell::Bit(None),
                MssqlCell::NVarChar(Some("second")),
                MssqlCell::VarBinary(None),
            ]
        );
    }

    #[test]
    fn converts_runtime_row_to_owned_tiberius_token_row() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("active", DataType::Boolean, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("payload", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(StringArray::from(vec![Some("first")])),
                Arc::new(BinaryArray::from(vec![Some(&b"abc"[..])])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let row = view.tiberius_row_owned(0).unwrap();

        assert_eq!(row.len(), 4);
        assert_eq!(row.get(0), Some(&tiberius::ColumnData::I32(Some(1))));
        assert_eq!(row.get(1), Some(&tiberius::ColumnData::Bit(Some(true))));

        let Some(tiberius::ColumnData::String(Some(Cow::Owned(value)))) = row.get(2) else {
            panic!("expected owned string column data");
        };
        assert_eq!(value, "first");

        let Some(tiberius::ColumnData::Binary(Some(Cow::Owned(value)))) = row.get(3) else {
            panic!("expected owned binary column data");
        };
        assert_eq!(value, b"abc");
    }

    #[test]
    fn row_helpers_reject_row_index_out_of_bounds() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_row(1).unwrap_err();
        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);

        let err = view.tiberius_row_owned(1).unwrap_err();
        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);
    }

    #[test]
    fn row_helpers_preserve_conversion_diagnostics() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![f64::NAN]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_row(0).unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(0),
            Some((0, "ratio")),
        );

        let err = view.tiberius_row_owned(0).unwrap_err();
        assert_single_diagnostic(
            err,
            DiagnosticCode::NonFiniteFloat,
            Some(0),
            Some((0, "ratio")),
        );
    }

    #[test]
    fn converts_mssql_cells_to_borrowed_tiberius_column_data() {
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Bit(Some(true))),
            tiberius::ColumnData::Bit(Some(true))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Bit(None)),
            tiberius::ColumnData::Bit(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::TinyInt(Some(8))),
            tiberius::ColumnData::U8(Some(8))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::TinyInt(None)),
            tiberius::ColumnData::U8(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::SmallInt(Some(-16))),
            tiberius::ColumnData::I16(Some(-16))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::SmallInt(None)),
            tiberius::ColumnData::I16(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Int(Some(32))),
            tiberius::ColumnData::I32(Some(32))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Int(None)),
            tiberius::ColumnData::I32(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::BigInt(Some(64))),
            tiberius::ColumnData::I64(Some(64))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::BigInt(None)),
            tiberius::ColumnData::I64(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Decimal(Some(MssqlDecimal::new(12345, 2)))),
            tiberius::ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(
                12345, 2
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Decimal(None)),
            tiberius::ColumnData::Numeric(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Real(Some(1.25))),
            tiberius::ColumnData::F32(Some(1.25))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Real(None)),
            tiberius::ColumnData::F32(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Float(Some(2.5))),
            tiberius::ColumnData::F64(Some(2.5))
        );
        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::Float(None)),
            tiberius::ColumnData::F64(None)
        );

        let text = "hello";
        let bytes = b"abc".as_slice();

        let text_data = mssql_cell_to_tiberius_borrowed(MssqlCell::NVarChar(Some(text)));
        let tiberius::ColumnData::String(Some(Cow::Borrowed(value))) = text_data else {
            panic!("expected borrowed string column data");
        };
        assert_eq!(value, text);

        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::NVarChar(None)),
            tiberius::ColumnData::String(None)
        );

        let binary_data = mssql_cell_to_tiberius_borrowed(MssqlCell::VarBinary(Some(bytes)));
        let tiberius::ColumnData::Binary(Some(Cow::Borrowed(value))) = binary_data else {
            panic!("expected borrowed binary column data");
        };
        assert_eq!(value, bytes);

        assert_eq!(
            mssql_cell_to_tiberius_borrowed(MssqlCell::VarBinary(None)),
            tiberius::ColumnData::Binary(None)
        );
    }

    #[test]
    fn converts_mssql_cells_to_owned_tiberius_column_data() {
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Bit(Some(true))),
            tiberius::ColumnData::Bit(Some(true))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::TinyInt(Some(8))),
            tiberius::ColumnData::U8(Some(8))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::SmallInt(Some(-16))),
            tiberius::ColumnData::I16(Some(-16))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Int(Some(32))),
            tiberius::ColumnData::I32(Some(32))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::BigInt(Some(64))),
            tiberius::ColumnData::I64(Some(64))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Decimal(Some(MssqlDecimal::new(12345, 2)))),
            tiberius::ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(
                12345, 2
            )))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Decimal(None)),
            tiberius::ColumnData::Numeric(None)
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Real(Some(1.25))),
            tiberius::ColumnData::F32(Some(1.25))
        );
        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::Float(Some(2.5))),
            tiberius::ColumnData::F64(Some(2.5))
        );

        let text_data = mssql_cell_to_tiberius_owned(MssqlCell::NVarChar(Some("hello")));
        let tiberius::ColumnData::String(Some(Cow::Owned(value))) = text_data else {
            panic!("expected owned string column data");
        };
        assert_eq!(value, "hello");

        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::NVarChar(None)),
            tiberius::ColumnData::String(None)
        );

        let binary_data = mssql_cell_to_tiberius_owned(MssqlCell::VarBinary(Some(b"abc")));
        let tiberius::ColumnData::Binary(Some(Cow::Owned(value))) = binary_data else {
            panic!("expected owned binary column data");
        };
        assert_eq!(value, b"abc");

        assert_eq!(
            mssql_cell_to_tiberius_owned(MssqlCell::VarBinary(None)),
            tiberius::ColumnData::Binary(None)
        );
    }

    #[test]
    fn converts_empty_ascii_and_non_ascii_strings() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("large_text", DataType::LargeUtf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["", "ascii", "東京"])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec!["", "ascii", "🙂"])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::NVarChar(Some("東京"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::NVarChar(Some(""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::NVarChar(Some("ascii"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 2).unwrap(),
            MssqlCell::NVarChar(Some("🙂"))
        );
    }

    #[test]
    fn converts_empty_and_non_empty_binary_values() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("bytes", DataType::Binary, true),
            Field::new("large_bytes", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("bytes", DataType::Binary, true),
                Field::new("large_bytes", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![Some(&b""[..]), Some(&b"abc"[..])])) as ArrayRef,
                Arc::new(LargeBinaryArray::from(vec![
                    Some(&b""[..]),
                    Some(&b"large"[..]),
                ])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"abc"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"large"))
        );
    }

    #[test]
    fn rejects_bounded_nvarchar_by_utf16_code_units() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("text", DataType::Utf8, true)]),
            PlanOptions {
                string_policy: StringPolicy::NVarChar(2),
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["ab", "🙂", "abc"]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::NVarChar(Some("ab"))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::NVarChar(Some("🙂"))
        );
        let err = view.mssql_cell(&mappings[0], 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "text")),
        );
    }

    #[test]
    fn rejects_bounded_varbinary_by_byte_count() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new("bytes", DataType::Binary, true)]),
            PlanOptions {
                binary_policy: BinaryPolicy::VarBinary(2),
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "bytes",
                DataType::Binary,
                true,
            )])),
            vec![Arc::new(BinaryArray::from(vec![
                Some(&b""[..]),
                Some(&b"ab"[..]),
                Some(&b"abc"[..]),
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::VarBinary(Some(b""))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::VarBinary(Some(b"ab"))
        );
        let err = view.mssql_cell(&mappings[0], 2).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTooLong,
            Some(2),
            Some((0, "bytes")),
        );
    }

    #[test]
    fn converts_uint64_decimal20_0_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                Some((i64::MAX as u64) + 1),
                Some(u64::MAX),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX), 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX) + 1, 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 3).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(u64::MAX), 0)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 4).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn converts_uint64_decimal20_0_to_owned_tiberius_numeric() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::Decimal20_0,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_decimal",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![Some(u64::MAX)]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let row = view.tiberius_row_owned(0).unwrap();

        assert_eq!(
            row.get(0),
            Some(&tiberius::ColumnData::Numeric(Some(
                tiberius::numeric::Numeric::new_with_scale(i128::from(u64::MAX), 0)
            )))
        );
    }

    #[test]
    fn converts_uint64_checked_bigint_boundary_values() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                true,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                Some(0_u64),
                Some(i64::MAX as u64),
                None,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::BigInt(Some(0))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::BigInt(Some(i64::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 2).unwrap(),
            MssqlCell::BigInt(None)
        );
    }

    #[test]
    fn rejects_uint64_checked_bigint_overflow_without_wrapping() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )]),
            PlanOptions {
                uint64_policy: UInt64Policy::CheckedBigInt,
                ..PlanOptions::default()
            },
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unsigned_as_bigint",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vec![
                (i64::MAX as u64) + 1,
                u64::MAX,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let just_over = view.mssql_cell(&mappings[0], 0).unwrap_err();
        assert_single_diagnostic(
            just_over,
            DiagnosticCode::IntegerOutOfRange,
            Some(0),
            Some((0, "unsigned_as_bigint")),
        );

        let max = view.mssql_cell(&mappings[0], 1).unwrap_err();
        assert_single_diagnostic(
            max,
            DiagnosticCode::IntegerOutOfRange,
            Some(1),
            Some((0, "unsigned_as_bigint")),
        );
    }

    #[test]
    fn rejects_policy_planned_decimal_runtime_conversion_until_implemented() {
        assert_policy_planned_null_runtime_unsupported(
            "amount",
            DataType::Decimal128(10, 2),
            PlanOptions::default(),
        );
    }

    #[test]
    fn rejects_policy_planned_date_runtime_conversion_until_implemented() {
        assert_policy_planned_null_runtime_unsupported(
            "date_value",
            DataType::Date32,
            PlanOptions::default(),
        );
        assert_policy_planned_null_runtime_unsupported(
            "date64_value",
            DataType::Date64,
            PlanOptions {
                date64_policy: Date64Policy::TimestampDateTime2,
                ..PlanOptions::default()
            },
        );
    }

    #[test]
    fn rejects_policy_planned_timestamp_runtime_conversion_until_implemented() {
        assert_policy_planned_null_runtime_unsupported(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            PlanOptions::default(),
        );
    }

    #[test]
    fn preserves_integer_boundaries_during_widening() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("tiny", DataType::Int8, false),
            Field::new("small", DataType::Int16, false),
            Field::new("unsigned_tiny", DataType::UInt8, false),
            Field::new("unsigned_medium", DataType::UInt16, false),
            Field::new("unsigned_large", DataType::UInt32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tiny", DataType::Int8, false),
                Field::new("small", DataType::Int16, false),
                Field::new("unsigned_tiny", DataType::UInt8, false),
                Field::new("unsigned_medium", DataType::UInt16, false),
                Field::new("unsigned_large", DataType::UInt32, false),
            ])),
            vec![
                Arc::new(Int8Array::from(vec![i8::MIN, i8::MAX])) as ArrayRef,
                Arc::new(Int16Array::from(vec![i16::MIN, i16::MAX])),
                Arc::new(UInt8Array::from(vec![u8::MIN, u8::MAX])),
                Arc::new(UInt16Array::from(vec![u16::MIN, u16::MAX])),
                Arc::new(UInt32Array::from(vec![u32::MIN, u32::MAX])),
            ],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        assert_eq!(
            view.mssql_cell(&mappings[0], 0).unwrap(),
            MssqlCell::SmallInt(Some(i16::from(i8::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[0], 1).unwrap(),
            MssqlCell::SmallInt(Some(i16::from(i8::MAX)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 0).unwrap(),
            MssqlCell::SmallInt(Some(i16::MIN))
        );
        assert_eq!(
            view.mssql_cell(&mappings[1], 1).unwrap(),
            MssqlCell::SmallInt(Some(i16::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 0).unwrap(),
            MssqlCell::TinyInt(Some(u8::MIN))
        );
        assert_eq!(
            view.mssql_cell(&mappings[2], 1).unwrap(),
            MssqlCell::TinyInt(Some(u8::MAX))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 0).unwrap(),
            MssqlCell::Int(Some(i32::from(u16::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[3], 1).unwrap(),
            MssqlCell::Int(Some(i32::from(u16::MAX)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 0).unwrap(),
            MssqlCell::BigInt(Some(i64::from(u32::MIN)))
        );
        assert_eq!(
            view.mssql_cell(&mappings[4], 1).unwrap(),
            MssqlCell::BigInt(Some(i64::from(u32::MAX)))
        );
    }

    #[test]
    fn rejects_null_in_non_nullable_planned_column() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "active",
            DataType::Boolean,
            false,
        )]));
        let batch = unsafe_batch_for_field(
            "active",
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![None::<bool>])),
            false,
        );
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::NullInNonNullableColumn,
            Some(0),
            Some((0, "active")),
        );
    }

    #[test]
    fn rejects_non_finite_float32_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float32,
                true,
            )])),
            vec![Arc::new(Float32Array::from(vec![
                f32::NAN,
                f32::INFINITY,
                f32::NEG_INFINITY,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        for row_index in 0..3 {
            let err = view.mssql_cell(&mappings[0], row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(row_index),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_non_finite_float64_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ratio",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        for row_index in 0..3 {
            let err = view.mssql_cell(&mappings[0], row_index).unwrap_err();

            assert_single_diagnostic(
                err,
                DiagnosticCode::NonFiniteFloat,
                Some(row_index),
                Some((0, "ratio")),
            );
        }
    }

    #[test]
    fn rejects_payload_that_does_not_fit_planned_mssql_type() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(0, "id".to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::BigInt, false),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![7_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueTypeMismatch,
            Some(0),
            Some((0, "id")),
        );
    }

    #[test]
    fn rejects_planned_column_index_outside_runtime_batch() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            None,
            Some((1, "active")),
        );
    }

    #[test]
    fn rejects_extra_runtime_columns_without_mappings() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("extra", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, None);
    }

    #[test]
    fn rejects_mapping_position_that_disagrees_with_arrow_index() {
        let mappings = vec![SchemaMapping::new(
            ArrowFieldRef::new(1, "id".to_owned(), false, DataType::Int32),
            MssqlColumn::new(Identifier::new("id").unwrap(), MssqlType::Int, false),
        )];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((1, "id")));
    }

    #[test]
    fn rejects_runtime_field_name_mismatch_even_when_type_matches() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Int32, false),
                Field::new("id", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![100_i32])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1_i32])),
            ],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((0, "id")));
    }

    #[test]
    fn rejects_runtime_field_rename_even_when_position_and_type_match() {
        let mappings =
            mappings_for_schema(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "renamed_id",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::SchemaMismatch, None, Some((0, "id")));
    }

    #[test]
    fn rejects_runtime_arrow_type_mismatch() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch = unsafe_batch_for_field(
            "number",
            DataType::Int32,
            Arc::new(Int64Array::from(vec![1_i64])),
            true,
        );

        let err = RecordBatchView::new(&batch, &mappings).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            None,
            Some((0, "number")),
        );
    }

    #[test]
    fn rejects_row_index_out_of_bounds() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "number",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.check_row_index(1).unwrap_err();

        assert_single_diagnostic(err, DiagnosticCode::RowIndexOutOfBounds, Some(1), None);
    }

    fn mappings_for_schema(schema: Schema) -> Vec<SchemaMapping> {
        mappings_for_schema_with_options(schema, PlanOptions::default())
    }

    fn mappings_for_schema_with_options(
        schema: Schema,
        options: PlanOptions,
    ) -> Vec<SchemaMapping> {
        plan_arrow_schema_to_mssql_mappings(
            Arc::new(schema),
            MssqlProfile::sql_server_2016_compat_100(),
            options,
        )
        .unwrap()
        .into_parts()
        .0
    }

    fn assert_policy_planned_null_runtime_unsupported(
        name: &str,
        data_type: DataType,
        options: PlanOptions,
    ) {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(name, data_type.clone(), true)]),
            options,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, data_type.clone(), true)])),
            vec![new_null_array(&data_type, 1)],
        )
        .unwrap();
        let view = RecordBatchView::new(&batch, &mappings).unwrap();

        let err = view.mssql_cell(&mappings[0], 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::ValueConversionUnsupported,
            Some(0),
            Some((0, name)),
        );
    }

    fn unsafe_batch_for_field(
        name: &str,
        data_type: DataType,
        array: ArrayRef,
        nullable: bool,
    ) -> RecordBatch {
        // SAFETY: this deliberately constructs a mismatched batch for converter
        // validation tests. The test only inspects metadata and never reads the
        // mismatched array through the declared schema type.
        unsafe {
            RecordBatch::new_unchecked(
                Arc::new(Schema::new(vec![Field::new(name, data_type, nullable)])),
                vec![array],
                1,
            )
        }
    }

    fn assert_single_diagnostic(
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
