use std::{fmt, str::FromStr};

use crate::{CompileError, ident::Ident};

/// A supported Snowflake SQL type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Number { precision: u8, scale: u8 },
    Float,
    Varchar,
    Binary,
    Date,
    Time,
    TimestampNtz,
    TimestampLtz,
    TimestampTz,
    Variant,
    Array,
    Object,
}

impl DataType {
    pub(crate) fn validate(self) -> Result<(), CompileError> {
        if let Self::Number { precision, scale } = self
            && (!(1..=38).contains(&precision) || scale > 37 || scale > precision)
        {
            return Err(CompileError::InvalidDataType(self.to_string()));
        }
        Ok(())
    }
}

impl FromStr for DataType {
    type Err = CompileError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let invalid = || CompileError::InvalidDataType(input.to_owned());
        let normalized = input.trim().to_ascii_uppercase();
        let (name, args) = match normalized.split_once('(') {
            Some((name, args)) => (
                name.trim_end(),
                Some(args.strip_suffix(')').ok_or_else(invalid)?),
            ),
            None => (normalized.as_str(), None),
        };
        let data_type = match (name, args) {
            ("NUMBER" | "DECIMAL" | "DEC" | "NUMERIC", args) => {
                let (precision, scale) = match args {
                    None => (38, 0),
                    Some(args) => {
                        let (precision, scale) = args.split_once(',').unwrap_or((args, "0"));
                        (
                            precision.trim().parse().map_err(|_| invalid())?,
                            scale.trim().parse().map_err(|_| invalid())?,
                        )
                    }
                };
                Self::Number { precision, scale }
            }
            ("INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "BYTEINT", None) => {
                Self::Number {
                    precision: 38,
                    scale: 0,
                }
            }
            ("VARCHAR" | "STRING" | "TEXT", None) => Self::Varchar,
            ("BOOLEAN" | "BOOL", None) => Self::Boolean,
            ("FLOAT" | "DOUBLE" | "DOUBLE PRECISION" | "REAL", None) => Self::Float,
            ("BINARY" | "VARBINARY", None) => Self::Binary,
            ("DATE", None) => Self::Date,
            ("TIME", None) => Self::Time,
            ("TIMESTAMP_NTZ", None) => Self::TimestampNtz,
            ("TIMESTAMP_LTZ", None) => Self::TimestampLtz,
            ("TIMESTAMP_TZ", None) => Self::TimestampTz,
            ("VARIANT", None) => Self::Variant,
            ("ARRAY", None) => Self::Array,
            ("OBJECT", None) => Self::Object,
            _ => return Err(invalid()),
        };
        data_type.validate()?;
        Ok(data_type)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Number { precision, scale } => return write!(f, "NUMBER({precision},{scale})"),
            Self::Boolean => "BOOLEAN",
            Self::Float => "FLOAT",
            Self::Varchar => "VARCHAR",
            Self::Binary => "BINARY",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::TimestampNtz => "TIMESTAMP_NTZ",
            Self::TimestampLtz => "TIMESTAMP_LTZ",
            Self::TimestampTz => "TIMESTAMP_TZ",
            Self::Variant => "VARIANT",
            Self::Array => "ARRAY",
            Self::Object => "OBJECT",
        })
    }
}

/// A named column with the type and nullability known to the local plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub(crate) name: Ident,
    pub data_type: Option<DataType>,
    pub nullable: Option<bool>,
}

impl Field {
    pub fn new(
        name: &str,
        data_type: Option<DataType>,
        nullable: Option<bool>,
    ) -> Result<Self, CompileError> {
        if let Some(data_type) = data_type {
            data_type.validate()?;
        }
        Ok(Self {
            name: Ident::parse(name)?,
            data_type,
            nullable,
        })
    }

    /// A Snowflake identifier, with double quotes where required to preserve its spelling.
    pub fn name(&self) -> String {
        self.name.normalized()
    }
}

/// An exact fixed-point value within Snowflake NUMBER's precision and scale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decimal {
    coefficient: i128,
    scale: u8,
}

impl Decimal {
    pub fn new(coefficient: i128, scale: u8) -> Result<Self, CompileError> {
        if coefficient.unsigned_abs() >= 10_u128.pow(38) || scale > 37 {
            return Err(CompileError::InvalidDecimal);
        }
        Ok(Self { coefficient, scale })
    }

    pub(crate) fn data_type(self) -> DataType {
        let digits = self
            .coefficient
            .unsigned_abs()
            .checked_ilog10()
            .unwrap_or(0) as u8
            + 1;
        DataType::Number {
            precision: digits.max(self.scale),
            scale: self.scale,
        }
    }
}

impl FromStr for Decimal {
    type Err = CompileError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (mantissa, exponent) = input.split_once(['e', 'E']).unwrap_or((input, "0"));
        let exponent: i32 = exponent.parse().map_err(|_| CompileError::InvalidDecimal)?;
        let negative = mantissa.starts_with('-');
        let digits = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
        let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
        if (whole.is_empty() && fraction.is_empty())
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|byte| byte.is_ascii_digit())
        {
            return Err(CompileError::InvalidDecimal);
        }
        let scale = i64::try_from(fraction.len()).map_err(|_| CompileError::InvalidDecimal)?
            - i64::from(exponent);
        let mut coefficient = format!("{whole}{fraction}")
            .parse::<i128>()
            .map_err(|_| CompileError::InvalidDecimal)?;
        if negative {
            coefficient = -coefficient;
        }
        if scale < 0 {
            if coefficient != 0 {
                coefficient = u32::try_from(-scale)
                    .ok()
                    .and_then(|exponent| 10_i128.checked_pow(exponent))
                    .and_then(|factor| coefficient.checked_mul(factor))
                    .ok_or(CompileError::InvalidDecimal)?;
            }
            return Self::new(coefficient, 0);
        }
        Self::new(
            coefficient,
            u8::try_from(scale).map_err(|_| CompileError::InvalidDecimal)?,
        )
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.coefficient < 0 {
            f.write_str("-")?;
        }
        let coefficient = self.coefficient.unsigned_abs();
        if self.scale == 0 {
            return write!(f, "{coefficient}");
        }
        let factor = 10_u128.pow(u32::from(self.scale));
        write!(
            f,
            "{}.{:0width$}",
            coefficient / factor,
            coefficient % factor,
            width = self.scale.into()
        )
    }
}
