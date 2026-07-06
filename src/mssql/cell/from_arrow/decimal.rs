//! Decimal Arrow-to-MSSQL runtime cell conversion.

use arrow_schema::DataType;

use crate::{
    DiagnosticCode, MssqlType, Result, SchemaMapping,
    arrow::cell::ArrowCell,
    conversion::arrow_to_mssql::{decimal::DecimalArrowToMssql, uint64::UInt64ArrowToMssql},
    mssql::cell::MssqlDecimal,
};

use super::{row_mapping_diagnostic, value_conversion_error};

pub(super) fn mssql_decimal_value(
    mapping: &SchemaMapping,
    row_index: usize,
    cell: ArrowCell<'_>,
) -> Result<MssqlDecimal> {
    if let DataType::UInt64 = mapping.arrow().data_type() {
        let scale = decimal_scale(mapping, row_index)?;
        validate_uint64_decimal_scale_compatibility(mapping, row_index, scale)?;
        let ArrowCell::UInt64(value) = cell else {
            return Err(value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::ValueTypeMismatch,
                format!("expected Arrow decimal-compatible payload, got {cell:?}"),
            )));
        };

        classify_uint64_decimal20_0(mapping, row_index)?;
        return mssql_decimal(mapping, row_index, i128::from(value), scale);
    }

    let classification = DecimalArrowToMssql::classify(mapping, row_index)?;
    match (cell, classification) {
        (ArrowCell::Decimal32(value), DecimalArrowToMssql::Decimal32 { .. }) => {
            let value = normalize_unscaled_decimal(
                mapping,
                row_index,
                i128::from(value),
                classification.arrow_scale(),
            )?;
            mssql_decimal_for_classification(mapping, row_index, value, classification)
        }
        (ArrowCell::Decimal64(value), DecimalArrowToMssql::Decimal64 { .. }) => {
            let value = normalize_unscaled_decimal(
                mapping,
                row_index,
                i128::from(value),
                classification.arrow_scale(),
            )?;
            mssql_decimal_for_classification(mapping, row_index, value, classification)
        }
        (ArrowCell::Decimal128(value), DecimalArrowToMssql::Decimal128 { .. }) => {
            let value = normalize_unscaled_decimal(
                mapping,
                row_index,
                value,
                classification.arrow_scale(),
            )?;
            mssql_decimal_for_classification(mapping, row_index, value, classification)
        }
        (ArrowCell::Decimal256(value), DecimalArrowToMssql::Decimal256CheckedDowncast { .. }) => {
            let value = value.to_i128().ok_or_else(|| {
                value_conversion_error(row_mapping_diagnostic(
                    mapping,
                    row_index,
                    DiagnosticCode::DecimalOutOfRange,
                    "Arrow Decimal256 value does not fit runtime i128 decimal representation",
                ))
            })?;
            let value = normalize_unscaled_decimal(
                mapping,
                row_index,
                value,
                classification.arrow_scale(),
            )?;
            mssql_decimal_for_classification(mapping, row_index, value, classification)
        }
        other => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            format!("expected Arrow decimal-compatible payload, got {other:?}"),
        ))),
    }
}

pub(super) fn supports_null_decimal_cell(mapping: &SchemaMapping) -> bool {
    match mapping.arrow().data_type() {
        DataType::UInt64 => is_uint64_decimal20_0_mapping(mapping),
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => matches!(mapping.mssql().ty(), MssqlType::Decimal { .. }),
        _ => false,
    }
}

fn is_uint64_decimal20_0_mapping(mapping: &SchemaMapping) -> bool {
    matches!(
        UInt64ArrowToMssql::classify(mapping, 0),
        Ok(UInt64ArrowToMssql::Decimal20_0)
    )
}

fn classify_uint64_decimal20_0(mapping: &SchemaMapping, row_index: usize) -> Result<()> {
    match UInt64ArrowToMssql::classify(mapping, row_index)? {
        UInt64ArrowToMssql::Decimal20_0 => Ok(()),
        UInt64ArrowToMssql::CheckedBigInt => Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned UInt64 conversion is not decimal(20,0)",
        ))),
    }
}

fn decimal_scale(mapping: &SchemaMapping, row_index: usize) -> Result<u8> {
    let MssqlType::Decimal { scale, .. } = mapping.mssql().ty() else {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned SQL Server type is not decimal",
        )));
    };

    let scale = u8::try_from(*scale).map_err(|_| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!(
                "planned SQL Server decimal scale {scale} cannot be represented by Tiberius Numeric"
            ),
        ))
    })?;

    if scale >= 38 {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!(
                "planned SQL Server decimal scale {scale} cannot be represented by Tiberius Numeric"
            ),
        )));
    }

    Ok(scale)
}

fn validate_uint64_decimal_scale_compatibility(
    mapping: &SchemaMapping,
    row_index: usize,
    planned_scale: u8,
) -> Result<()> {
    classify_uint64_decimal20_0(mapping, row_index)?;
    let expected_scale = 0;

    if planned_scale == expected_scale {
        return Ok(());
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::SchemaMismatch,
        format!(
            "planned SQL Server decimal scale {planned_scale} is incompatible with Arrow decimal scale {expected_scale}"
        ),
    )))
}

fn mssql_decimal_for_classification(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    classification: DecimalArrowToMssql,
) -> Result<MssqlDecimal> {
    mssql_decimal_with_shape(
        mapping,
        row_index,
        unscaled,
        classification.target_precision(),
        classification.target_scale(),
    )
}

fn mssql_decimal(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    scale: u8,
) -> Result<MssqlDecimal> {
    let MssqlType::Decimal { precision, .. } = mapping.mssql().ty() else {
        return Err(value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::ValueTypeMismatch,
            "planned SQL Server type is not decimal",
        )));
    };

    mssql_decimal_with_shape(mapping, row_index, unscaled, *precision, scale)
}

fn mssql_decimal_with_shape(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    precision: u8,
    scale: u8,
) -> Result<MssqlDecimal> {
    if decimal_unscaled_fits_precision(unscaled, precision) {
        return Ok(MssqlDecimal::new(unscaled, scale));
    }

    Err(value_conversion_error(row_mapping_diagnostic(
        mapping,
        row_index,
        DiagnosticCode::DecimalOutOfRange,
        format!("decimal value {unscaled} does not fit planned precision {precision}"),
    )))
}

fn normalize_unscaled_decimal(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    arrow_scale: i8,
) -> Result<i128> {
    if arrow_scale >= 0 {
        Ok(unscaled)
    } else {
        normalize_negative_scale(mapping, row_index, unscaled, arrow_scale)
    }
}

fn normalize_negative_scale(
    mapping: &SchemaMapping,
    row_index: usize,
    unscaled: i128,
    arrow_scale: i8,
) -> Result<i128> {
    if arrow_scale >= 0 {
        return Ok(unscaled);
    }

    let factor = 10_i128
        .checked_pow(u32::from(arrow_scale.unsigned_abs()))
        .ok_or_else(|| {
            value_conversion_error(row_mapping_diagnostic(
                mapping,
                row_index,
                DiagnosticCode::DecimalOutOfRange,
                format!("negative decimal scale {arrow_scale} normalization factor overflows"),
            ))
        })?;

    unscaled.checked_mul(factor).ok_or_else(|| {
        value_conversion_error(row_mapping_diagnostic(
            mapping,
            row_index,
            DiagnosticCode::DecimalOutOfRange,
            format!("decimal value {unscaled} overflows while normalizing scale {arrow_scale}"),
        ))
    })
}

fn decimal_unscaled_fits_precision(value: i128, precision: u8) -> bool {
    if precision == 0 {
        return false;
    }

    let Some(max) = decimal_max_unscaled(precision) else {
        return false;
    };

    value <= max && value >= -max
}

fn decimal_max_unscaled(precision: u8) -> Option<i128> {
    10_i128.checked_pow(u32::from(precision))?.checked_sub(1)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_buffer::i256;
    use arrow_schema::{DataType, Field, Schema};

    use super::super::{ArrowToMssqlRuntimeMapping, mssql_cell_from_arrow_cell};
    use crate::{
        ArrowFieldRef, DecimalPolicy, DiagnosticCode, Identifier, MssqlColumn, MssqlProfile,
        MssqlType, PlanOptions, SchemaMapping, UInt64Policy,
        arrow::cell::ArrowCell,
        mssql::cell::{MssqlCell, MssqlDecimal},
        plan_arrow_schema_to_mssql_mappings,
    };

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

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(0), 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(i64::MAX as u64), 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX), 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64((i64::MAX as u64) + 1), 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(i64::MAX) + 1, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::UInt64(u64::MAX), 3).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(i128::from(u64::MAX), 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 4).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn converts_decimal32_64_128_cells_with_sign_zero_scale_and_null() {
        let mappings = mappings_for_schema(Schema::new(vec![
            Field::new("decimal32", DataType::Decimal32(9, 2), true),
            Field::new("decimal64", DataType::Decimal64(18, 4), true),
            Field::new("decimal128", DataType::Decimal128(38, 9), true),
        ]));

        let cases = [
            (
                0,
                ArrowCell::Decimal32(12_345),
                MssqlCell::Decimal(Some(MssqlDecimal::new(12_345, 2))),
            ),
            (
                0,
                ArrowCell::Decimal32(-12_345),
                MssqlCell::Decimal(Some(MssqlDecimal::new(-12_345, 2))),
            ),
            (
                0,
                ArrowCell::Decimal32(0),
                MssqlCell::Decimal(Some(MssqlDecimal::new(0, 2))),
            ),
            (
                1,
                ArrowCell::Decimal64(1_234_567_890),
                MssqlCell::Decimal(Some(MssqlDecimal::new(1_234_567_890, 4))),
            ),
            (
                1,
                ArrowCell::Decimal64(-1_234_567_890),
                MssqlCell::Decimal(Some(MssqlDecimal::new(-1_234_567_890, 4))),
            ),
            (
                1,
                ArrowCell::Decimal64(0),
                MssqlCell::Decimal(Some(MssqlDecimal::new(0, 4))),
            ),
            (
                2,
                ArrowCell::Decimal128(123_456_789_012_345_678_901_234_567_890),
                MssqlCell::Decimal(Some(MssqlDecimal::new(
                    123_456_789_012_345_678_901_234_567_890,
                    9,
                ))),
            ),
            (
                2,
                ArrowCell::Decimal128(-123_456_789_012_345_678_901_234_567_890),
                MssqlCell::Decimal(Some(MssqlDecimal::new(
                    -123_456_789_012_345_678_901_234_567_890,
                    9,
                ))),
            ),
            (
                2,
                ArrowCell::Decimal128(0),
                MssqlCell::Decimal(Some(MssqlDecimal::new(0, 9))),
            ),
        ];

        for (index, cell, expected) in cases {
            assert_eq!(convert_cell(&mappings[index], cell, 0).unwrap(), expected);
        }

        for mapping in mappings.iter().take(3) {
            assert_eq!(
                convert_cell(mapping, ArrowCell::Null, 3).unwrap(),
                MssqlCell::Decimal(None)
            );
        }
    }

    #[test]
    fn normalizes_negative_decimal_scale_at_runtime() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(3, -2),
                true,
            )]),
            PlanOptions {
                decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                ..PlanOptions::default()
            },
        );

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(123), 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(12_300, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(-123), 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-12_300, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(0), 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 0)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn rejects_negative_decimal_scale_normalization_overflow() {
        let mappings = mappings_for_schema_with_options(
            Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(37, -1),
                false,
            )]),
            PlanOptions {
                decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                ..PlanOptions::default()
            },
        );

        let err = convert_cell(&mappings[0], ArrowCell::Decimal128(i128::MAX), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_decimal_scale_that_tiberius_numeric_cannot_represent() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 38),
            true,
        )]));

        let err = convert_cell(&mappings[0], ArrowCell::Decimal128(1), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_decimal_mapping_scale_mismatch_before_value_corruption() {
        let mapping = SchemaMapping::new(
            ArrowFieldRef::new(0, "amount".to_owned(), false, DataType::Decimal128(5, 2)),
            MssqlColumn::new(
                Identifier::new("amount").unwrap(),
                MssqlType::Decimal {
                    precision: 5,
                    scale: 0,
                },
                false,
            ),
        );

        let err = convert_cell(&mapping, ArrowCell::Decimal128(123), 0).unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::SchemaMismatch,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn accepts_decimal_values_at_planned_precision_boundaries() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(5, 2),
            false,
        )]));

        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(99_999), 0).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(99_999, 2)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal128(-99_999), 1).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(-99_999, 2)))
        );
    }

    #[test]
    fn rejects_decimal_values_outside_planned_precision() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(5, 2),
            false,
        )]));

        let positive = convert_cell(&mappings[0], ArrowCell::Decimal128(100_000), 0).unwrap_err();
        assert_single_diagnostic(
            positive,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );

        let negative = convert_cell(&mappings[0], ArrowCell::Decimal128(-100_000), 1).unwrap_err();
        assert_single_diagnostic(
            negative,
            DiagnosticCode::DecimalOutOfRange,
            Some(1),
            Some((0, "amount")),
        );
    }

    #[test]
    fn converts_decimal256_checked_downcast_values() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(38, 4),
            true,
        )]));

        assert_eq!(
            convert_cell(
                &mappings[0],
                ArrowCell::Decimal256(i256::from_i128(123_456_789_012_345_678_901_234_567_890)),
                0,
            )
            .unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                123_456_789_012_345_678_901_234_567_890,
                4,
            )))
        );
        assert_eq!(
            convert_cell(
                &mappings[0],
                ArrowCell::Decimal256(i256::from_i128(-123_456_789_012_345_678_901_234_567_890)),
                1,
            )
            .unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(
                -123_456_789_012_345_678_901_234_567_890,
                4,
            )))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Decimal256(i256::ZERO), 2).unwrap(),
            MssqlCell::Decimal(Some(MssqlDecimal::new(0, 4)))
        );
        assert_eq!(
            convert_cell(&mappings[0], ArrowCell::Null, 3).unwrap(),
            MssqlCell::Decimal(None)
        );
    }

    #[test]
    fn rejects_decimal256_values_that_do_not_fit_i128_runtime_representation() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(38, 0),
            false,
        )]));

        let err = convert_cell(
            &mappings[0],
            ArrowCell::Decimal256(i256::from_i128(i128::MAX) + i256::ONE),
            0,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    #[test]
    fn rejects_decimal256_checked_downcast_values_outside_planned_precision() {
        let mappings = mappings_for_schema(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(5, 2),
            false,
        )]));

        let err = convert_cell(
            &mappings[0],
            ArrowCell::Decimal256(i256::from_i128(100_000)),
            0,
        )
        .unwrap_err();

        assert_single_diagnostic(
            err,
            DiagnosticCode::DecimalOutOfRange,
            Some(0),
            Some((0, "amount")),
        );
    }

    fn convert_cell<'a>(
        mapping: &SchemaMapping,
        cell: ArrowCell<'a>,
        row_index: usize,
    ) -> crate::Result<MssqlCell<'a>> {
        let options = PlanOptions::default();
        let runtime_mapping = ArrowToMssqlRuntimeMapping::new_with_options(mapping, &options);
        mssql_cell_from_arrow_cell(runtime_mapping, cell, row_index)
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

    fn assert_single_diagnostic(
        err: crate::Error,
        expected_code: DiagnosticCode,
        expected_row: Option<usize>,
        expected_field: Option<(usize, &str)>,
    ) {
        let crate::Error::ValueConversion { diagnostics } = err else {
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
