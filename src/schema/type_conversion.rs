//! Arrow schema type to MSSQL type planning.

use arrow_schema::{DataType, Field, TimeUnit};

use crate::write::{
    BinaryPolicy, Date64Policy, Decimal256Policy, DecimalPolicy, PlanOptions, StringPolicy,
    TimezonePolicy, UInt64Policy,
};
use crate::{Diagnostic, DiagnosticCode, FieldRef, MssqlType, MssqlTypeLength};

pub(crate) fn plan_arrow_data_type_as_mssql_type(
    index: usize,
    field: &Field,
    options: &PlanOptions,
) -> Result<MssqlType, Diagnostic> {
    match field.data_type() {
        DataType::Boolean => Ok(MssqlType::Bit),
        DataType::Int8 | DataType::Int16 => Ok(MssqlType::SmallInt),
        DataType::Int32 => Ok(MssqlType::Int),
        DataType::Int64 => Ok(MssqlType::BigInt),
        DataType::UInt8 => Ok(MssqlType::TinyInt),
        DataType::UInt16 => Ok(MssqlType::Int),
        DataType::UInt32 => Ok(MssqlType::BigInt),
        DataType::UInt64 => plan_arrow_uint64_as_mssql_type(options.uint64_policy, index, field),
        DataType::Float32 => Ok(MssqlType::Real),
        DataType::Float64 => Ok(MssqlType::Float { precision: 53 }),
        DataType::Utf8 | DataType::LargeUtf8 => {
            plan_arrow_string_as_mssql_type(options.string_policy, index, field)
        }
        DataType::Binary | DataType::LargeBinary => {
            plan_arrow_binary_as_mssql_type(options.binary_policy, index, field)
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => plan_arrow_decimal_as_mssql_type(
            *precision,
            *scale,
            options.decimal_policy,
            index,
            field,
        ),
        DataType::Decimal256(precision, scale) => plan_arrow_decimal256_as_mssql_type(
            *precision,
            *scale,
            options.decimal_policy,
            options.decimal256_policy,
            index,
            field,
        ),
        DataType::Date32 => Ok(MssqlType::Date),
        DataType::Date64 => plan_arrow_date64_as_mssql_type(options.date64_policy, index, field),
        DataType::Time32(TimeUnit::Second) => Ok(MssqlType::Time { precision: 0 }),
        DataType::Time32(TimeUnit::Millisecond) => Ok(MssqlType::Time { precision: 3 }),
        DataType::Time64(TimeUnit::Microsecond) => Ok(MssqlType::Time { precision: 6 }),
        DataType::Time64(TimeUnit::Nanosecond) => Ok(MssqlType::Time { precision: 7 }),
        DataType::Timestamp(_, timezone) => plan_arrow_timestamp_as_mssql_type(
            timezone.as_deref(),
            options.timezone_policy,
            index,
            field,
        ),
        other => Err(unsupported_arrow_type_for_arrow_to_mssql(
            index, field, other,
        )),
    }
}

fn plan_arrow_uint64_as_mssql_type(
    policy: UInt64Policy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match policy {
        UInt64Policy::Reject => Err(policy_required_for_arrow_to_mssql(
            index,
            field,
            "UInt64 requires UInt64Policy::Decimal20_0 or UInt64Policy::CheckedBigInt",
        )),
        UInt64Policy::Decimal20_0 => Ok(MssqlType::Decimal {
            precision: 20,
            scale: 0,
        }),
        UInt64Policy::CheckedBigInt => Ok(MssqlType::BigInt),
    }
}

fn plan_arrow_string_as_mssql_type(
    policy: StringPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match policy {
        StringPolicy::NVarCharMax => Ok(MssqlType::NVarChar(MssqlTypeLength::Max)),
        StringPolicy::NVarChar(length) => Ok(MssqlType::NVarChar(MssqlTypeLength::Bounded(length))),
        StringPolicy::ObservedNVarChar => Err(observed_data_required_for_arrow_to_mssql(
            index,
            field,
            "ObservedNVarChar requires observed values or statistics",
        )),
    }
}

fn plan_arrow_binary_as_mssql_type(
    policy: BinaryPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match policy {
        BinaryPolicy::VarBinaryMax => Ok(MssqlType::VarBinary(MssqlTypeLength::Max)),
        BinaryPolicy::VarBinary(length) => {
            Ok(MssqlType::VarBinary(MssqlTypeLength::Bounded(length)))
        }
        BinaryPolicy::ObservedVarBinary => Err(observed_data_required_for_arrow_to_mssql(
            index,
            field,
            "ObservedVarBinary requires observed values or statistics",
        )),
    }
}

fn plan_arrow_decimal_as_mssql_type(
    precision: u8,
    scale: i8,
    policy: DecimalPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    let (precision, scale) = normalize_arrow_decimal_shape(precision, scale, policy, index, field)?;
    Ok(MssqlType::Decimal { precision, scale })
}

fn plan_arrow_decimal256_as_mssql_type(
    precision: u8,
    scale: i8,
    decimal_policy: DecimalPolicy,
    decimal256_policy: Decimal256Policy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match decimal256_policy {
        Decimal256Policy::CheckedDowncast => {
            plan_arrow_decimal_as_mssql_type(precision, scale, decimal_policy, index, field)
        }
        Decimal256Policy::Reject => Err(policy_required_for_arrow_to_mssql(
            index,
            field,
            "Decimal256 requires Decimal256Policy::CheckedDowncast",
        )),
    }
}

fn normalize_arrow_decimal_shape(
    precision: u8,
    scale: i8,
    policy: DecimalPolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<(u8, i8), Diagnostic> {
    if precision == 0 {
        return Err(decimal_out_of_range_for_arrow_to_mssql(
            index,
            field,
            "decimal precision must be at least 1 for SQL Server",
        ));
    }

    let (precision, scale) = if scale < 0 {
        match policy {
            DecimalPolicy::RejectNegativeScale => {
                return Err(policy_required_for_arrow_to_mssql(
                    index,
                    field,
                    "negative decimal scale requires DecimalPolicy::NormalizeNegativeScale",
                ));
            }
            DecimalPolicy::NormalizeNegativeScale => {
                let extra_digits = scale.unsigned_abs();
                let Some(normalized_precision) = precision.checked_add(extra_digits) else {
                    return Err(decimal_out_of_range_for_arrow_to_mssql(
                        index,
                        field,
                        "normalized decimal precision overflows u8",
                    ));
                };
                (normalized_precision, 0)
            }
        }
    } else {
        (precision, scale)
    };

    if precision > SQL_SERVER_MAX_DECIMAL_PRECISION {
        return Err(decimal_out_of_range_for_arrow_to_mssql(
            index,
            field,
            format!("decimal precision {precision} exceeds SQL Server maximum precision 38"),
        ));
    }

    let scale_as_u8 = u8::try_from(scale).map_err(|_| {
        decimal_out_of_range_for_arrow_to_mssql(
            index,
            field,
            format!("decimal scale {scale} must be non-negative"),
        )
    })?;

    if scale_as_u8 > precision {
        return Err(decimal_out_of_range_for_arrow_to_mssql(
            index,
            field,
            format!("decimal scale {scale} cannot exceed precision {precision}"),
        ));
    }

    Ok((precision, scale))
}

fn plan_arrow_date64_as_mssql_type(
    policy: Date64Policy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    match policy {
        Date64Policy::RejectNonMidnight => Err(observed_data_required_for_arrow_to_mssql(
            index,
            field,
            "Date64 requires observed values to verify every value is midnight",
        )),
        Date64Policy::TimestampDateTime2 => Ok(MssqlType::DateTime2 { precision: 3 }),
    }
}

fn plan_arrow_timestamp_as_mssql_type(
    timezone: Option<&str>,
    timezone_policy: TimezonePolicy,
    index: usize,
    field: &Field,
) -> std::result::Result<MssqlType, Diagnostic> {
    let has_timezone = timezone.is_some_and(|timezone| !timezone.is_empty());

    if has_timezone && matches!(timezone_policy, TimezonePolicy::Reject) {
        return Err(policy_required_for_arrow_to_mssql(
            index,
            field,
            "timezone-aware timestamps require TimezonePolicy::DateTimeOffset or TimezonePolicy::NormalizeUtcDateTime2",
        ));
    }

    let ty = if has_timezone && matches!(timezone_policy, TimezonePolicy::DateTimeOffset) {
        MssqlType::DateTimeOffset { precision: 7 }
    } else {
        MssqlType::DateTime2 { precision: 7 }
    };

    Ok(ty)
}

fn policy_required_for_arrow_to_mssql(
    index: usize,
    field: &Field,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::ProfileDependentConversion, message)
        .with_field(FieldRef::new(index, field.name()))
}

fn observed_data_required_for_arrow_to_mssql(
    index: usize,
    field: &Field,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::ObservedDataRequired, message)
        .with_field(FieldRef::new(index, field.name()))
}

fn decimal_out_of_range_for_arrow_to_mssql(
    index: usize,
    field: &Field,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::error(DiagnosticCode::DecimalOutOfRange, message)
        .with_field(FieldRef::new(index, field.name()))
}

fn unsupported_arrow_type_for_arrow_to_mssql(
    index: usize,
    field: &Field,
    data_type: &DataType,
) -> Diagnostic {
    let family = unsupported_arrow_type_family(data_type);
    Diagnostic::error(
        DiagnosticCode::UnsupportedArrowType,
        format!("{family} Arrow type {data_type:?} is not supported for Arrow-to-MSSQL planning"),
    )
    .with_field(FieldRef::new(index, field.name()))
}

fn unsupported_arrow_type_family(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Null => "null",
        DataType::Float16 => "16-bit floating-point",
        DataType::Time32(_) | DataType::Time64(_) => "time-only",
        DataType::Duration(_) => "duration",
        DataType::Interval(_) => "interval",
        DataType::FixedSizeBinary(_) => "fixed-size binary",
        DataType::BinaryView | DataType::Utf8View => "view",
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => "nested",
        DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _) => "encoded",
        _ => "unsupported",
    }
}

const SQL_SERVER_MAX_DECIMAL_PRECISION: u8 = 38;

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, TimeUnit};

    use super::plan_arrow_data_type_as_mssql_type;
    use crate::{
        Date64Policy, Decimal256Policy, DecimalPolicy, Diagnostic, DiagnosticCode, MssqlType,
        MssqlTypeLength, NanosecondPolicy, PlanOptions, StringPolicy, TimezonePolicy, UInt64Policy,
    };

    #[test]
    fn maps_integer_float_string_and_binary_types() {
        let cases = [
            (DataType::Boolean, MssqlType::Bit),
            (DataType::Int8, MssqlType::SmallInt),
            (DataType::Int16, MssqlType::SmallInt),
            (DataType::Int32, MssqlType::Int),
            (DataType::Int64, MssqlType::BigInt),
            (DataType::UInt8, MssqlType::TinyInt),
            (DataType::UInt16, MssqlType::Int),
            (DataType::UInt32, MssqlType::BigInt),
            (DataType::Float32, MssqlType::Real),
            (DataType::Float64, MssqlType::Float { precision: 53 }),
            (DataType::Utf8, MssqlType::NVarChar(MssqlTypeLength::Max)),
            (
                DataType::LargeUtf8,
                MssqlType::NVarChar(MssqlTypeLength::Max),
            ),
            (DataType::Binary, MssqlType::VarBinary(MssqlTypeLength::Max)),
            (
                DataType::LargeBinary,
                MssqlType::VarBinary(MssqlTypeLength::Max),
            ),
        ];

        for (data_type, expected) in cases {
            assert_eq!(
                plan_type(data_type, PlanOptions::default()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn applies_bounded_string_and_binary_policies() {
        assert_eq!(
            plan_type(
                DataType::Utf8,
                PlanOptions {
                    string_policy: StringPolicy::NVarChar(128),
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlType::NVarChar(MssqlTypeLength::Bounded(128))
        );
        assert_eq!(
            plan_type(
                DataType::Binary,
                PlanOptions {
                    binary_policy: crate::BinaryPolicy::VarBinary(256),
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlType::VarBinary(MssqlTypeLength::Bounded(256))
        );
    }

    #[test]
    fn maps_uint64_when_explicit_policy_is_selected() {
        assert_eq!(
            plan_type(
                DataType::UInt64,
                PlanOptions {
                    uint64_policy: UInt64Policy::Decimal20_0,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlType::Decimal {
                precision: 20,
                scale: 0
            }
        );
        assert_eq!(
            plan_type(
                DataType::UInt64,
                PlanOptions {
                    uint64_policy: UInt64Policy::CheckedBigInt,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlType::BigInt
        );
    }

    #[test]
    fn default_uint64_policy_returns_structured_planning_diagnostic() {
        let diagnostic = plan_type(DataType::UInt64, PlanOptions::default()).unwrap_err();

        assert_eq!(
            diagnostic.code(),
            DiagnosticCode::ProfileDependentConversion
        );
        assert_eq!(diagnostic.field().unwrap().index(), 0);
        assert_eq!(diagnostic.field().unwrap().name(), "value");
    }

    #[test]
    fn observed_length_policies_return_structured_planning_diagnostics() {
        let string_diagnostic = plan_type(
            DataType::Utf8,
            PlanOptions {
                string_policy: StringPolicy::ObservedNVarChar,
                ..PlanOptions::default()
            },
        )
        .unwrap_err();
        let binary_diagnostic = plan_type(
            DataType::Binary,
            PlanOptions {
                binary_policy: crate::BinaryPolicy::ObservedVarBinary,
                ..PlanOptions::default()
            },
        )
        .unwrap_err();

        assert_eq!(
            string_diagnostic.code(),
            DiagnosticCode::ObservedDataRequired
        );
        assert_eq!(string_diagnostic.field().unwrap().name(), "value");
        assert_eq!(
            binary_diagnostic.code(),
            DiagnosticCode::ObservedDataRequired
        );
        assert_eq!(binary_diagnostic.field().unwrap().name(), "value");
    }

    #[test]
    fn maps_decimal_date_and_timestamp_types() {
        let cases = [
            (
                DataType::Decimal32(9, 2),
                MssqlType::Decimal {
                    precision: 9,
                    scale: 2,
                },
            ),
            (
                DataType::Decimal64(18, 4),
                MssqlType::Decimal {
                    precision: 18,
                    scale: 4,
                },
            ),
            (
                DataType::Decimal128(38, 9),
                MssqlType::Decimal {
                    precision: 38,
                    scale: 9,
                },
            ),
            (
                DataType::Decimal256(38, 0),
                MssqlType::Decimal {
                    precision: 38,
                    scale: 0,
                },
            ),
            (DataType::Date32, MssqlType::Date),
            (
                DataType::Timestamp(TimeUnit::Second, None),
                MssqlType::DateTime2 { precision: 7 },
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                MssqlType::DateTime2 { precision: 7 },
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                MssqlType::DateTime2 { precision: 7 },
            ),
            (
                DataType::Timestamp(TimeUnit::Second, Some("".into())),
                MssqlType::DateTime2 { precision: 7 },
            ),
            (
                DataType::Time32(TimeUnit::Second),
                MssqlType::Time { precision: 0 },
            ),
            (
                DataType::Time32(TimeUnit::Millisecond),
                MssqlType::Time { precision: 3 },
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                MssqlType::Time { precision: 6 },
            ),
            (
                DataType::Time64(TimeUnit::Nanosecond),
                MssqlType::Time { precision: 7 },
            ),
        ];

        for (data_type, expected) in cases {
            assert_eq!(
                plan_type(data_type, PlanOptions::default()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn normalizes_negative_decimal_scale_when_policy_is_selected() {
        assert_eq!(
            plan_type(
                DataType::Decimal128(3, -2),
                PlanOptions {
                    decimal_policy: DecimalPolicy::NormalizeNegativeScale,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlType::Decimal {
                precision: 5,
                scale: 0
            }
        );
    }

    #[test]
    fn rejects_decimal_shapes_that_sql_server_cannot_represent() {
        assert_diagnostic(
            plan_type(DataType::Decimal256(39, 0), PlanOptions::default()).unwrap_err(),
            DiagnosticCode::DecimalOutOfRange,
        );
        assert_diagnostic(
            plan_type(DataType::Decimal128(3, 4), PlanOptions::default()).unwrap_err(),
            DiagnosticCode::DecimalOutOfRange,
        );
        assert_diagnostic(
            plan_type(DataType::Decimal128(3, -2), PlanOptions::default()).unwrap_err(),
            DiagnosticCode::ProfileDependentConversion,
        );
    }

    #[test]
    fn decimal256_reject_policy_returns_structured_planning_diagnostic() {
        let diagnostic = plan_type(
            DataType::Decimal256(38, 0),
            PlanOptions {
                decimal256_policy: Decimal256Policy::Reject,
                ..PlanOptions::default()
            },
        )
        .unwrap_err();

        assert_diagnostic(diagnostic, DiagnosticCode::ProfileDependentConversion);
    }

    #[test]
    fn date64_requires_policy_or_observed_midnight_validation() {
        assert_diagnostic(
            plan_type(DataType::Date64, PlanOptions::default()).unwrap_err(),
            DiagnosticCode::ObservedDataRequired,
        );
        assert_eq!(
            plan_type(
                DataType::Date64,
                PlanOptions {
                    date64_policy: Date64Policy::TimestampDateTime2,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlType::DateTime2 { precision: 3 }
        );
    }

    #[test]
    fn timezone_timestamp_policy_controls_target_type() {
        let data_type = DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into()));

        assert_diagnostic(
            plan_type(data_type.clone(), PlanOptions::default()).unwrap_err(),
            DiagnosticCode::ProfileDependentConversion,
        );
        assert_eq!(
            plan_type(
                data_type.clone(),
                PlanOptions {
                    timezone_policy: TimezonePolicy::DateTimeOffset,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlType::DateTimeOffset { precision: 7 }
        );
        assert_eq!(
            plan_type(
                data_type,
                PlanOptions {
                    timezone_policy: TimezonePolicy::NormalizeUtcDateTime2,
                    ..PlanOptions::default()
                },
            )
            .unwrap(),
            MssqlType::DateTime2 { precision: 7 }
        );
    }

    #[test]
    fn nanosecond_timestamp_policy_is_enforced_at_runtime() {
        let data_type = DataType::Timestamp(TimeUnit::Nanosecond, None);

        assert_eq!(
            plan_type(data_type.clone(), PlanOptions::default()).unwrap(),
            MssqlType::DateTime2 { precision: 7 }
        );

        for nanosecond_policy in [
            NanosecondPolicy::RejectNon100ns,
            NanosecondPolicy::RoundTo100ns,
            NanosecondPolicy::TruncateTo100ns,
        ] {
            assert_eq!(
                plan_type(
                    data_type.clone(),
                    PlanOptions {
                        nanosecond_policy,
                        ..PlanOptions::default()
                    },
                )
                .unwrap(),
                MssqlType::DateTime2 { precision: 7 }
            );
        }
    }

    #[test]
    fn unsupported_type_returns_structured_planning_diagnostic() {
        let diagnostic = plan_type(
            DataType::new_list(DataType::Int32, true),
            PlanOptions::default(),
        )
        .unwrap_err();

        assert_eq!(diagnostic.code(), DiagnosticCode::UnsupportedArrowType);
        assert_eq!(diagnostic.field().unwrap().index(), 0);
        assert_eq!(diagnostic.field().unwrap().name(), "value");
    }

    #[test]
    fn unsupported_type_diagnostics_name_arrow_type_family() {
        let cases = [
            (DataType::Null, "null"),
            (DataType::Float16, "16-bit floating-point"),
            (DataType::Time32(TimeUnit::Microsecond), "time-only"),
            (DataType::Duration(TimeUnit::Microsecond), "duration"),
            (DataType::FixedSizeBinary(16), "fixed-size binary"),
            (DataType::BinaryView, "view"),
            (DataType::Utf8View, "view"),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                "encoded",
            ),
        ];

        for (data_type, family) in cases {
            let diagnostic = plan_type(data_type, PlanOptions::default()).unwrap_err();
            assert_eq!(diagnostic.code(), DiagnosticCode::UnsupportedArrowType);
            assert!(
                diagnostic.message().contains(family),
                "diagnostic should mention family {family:?}: {}",
                diagnostic.message()
            );
        }
    }

    fn plan_type(
        data_type: DataType,
        options: PlanOptions,
    ) -> std::result::Result<MssqlType, Diagnostic> {
        let field = Field::new("value", data_type, true);
        plan_arrow_data_type_as_mssql_type(0, &field, &options)
    }

    fn assert_diagnostic(diagnostic: Diagnostic, expected_code: DiagnosticCode) {
        assert_eq!(diagnostic.code(), expected_code);
        assert_eq!(diagnostic.field().unwrap().index(), 0);
        assert_eq!(diagnostic.field().unwrap().name(), "value");
    }
}
