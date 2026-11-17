use std::fmt;

use crate::CompileError;

/// A resolved Snowflake identifier with its original quoting semantics.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Ident(String);

impl Ident {
    pub(crate) fn parse(input: &str) -> Result<Self, CompileError> {
        let invalid = || CompileError::InvalidIdentifier(input.to_owned());
        let name = if let Some(quoted) = input.strip_prefix('"') {
            let mut chars = quoted.strip_suffix('"').ok_or_else(invalid)?.chars();
            let mut name = String::new();
            while let Some(ch) = chars.next() {
                if ch == '"' && chars.next() != Some('"') {
                    return Err(invalid());
                }
                name.push(ch);
            }
            name
        } else {
            let mut bytes = input.bytes();
            let valid_start = bytes
                .next()
                .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == b'_');
            if !valid_start
                || !bytes.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'_' | b'$'))
            {
                return Err(invalid());
            }
            input.to_ascii_uppercase()
        };
        if name.is_empty() || name.contains('\0') || name.chars().count() > 255 {
            return Err(invalid());
        }
        Ok(Self(name))
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.0.replace('"', "\"\""))
    }
}

pub(crate) struct ObjectName(Vec<Ident>);

impl ObjectName {
    pub(crate) fn parse(
        parts: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, CompileError> {
        let parts = parts
            .into_iter()
            .map(|part| Ident::parse(part.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        if !(1..=3).contains(&parts.len()) {
            return Err(CompileError::InvalidTableName);
        }
        Ok(Self(parts))
    }
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, part) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(".")?;
            }
            part.fmt(f)?;
        }
        Ok(())
    }
}
