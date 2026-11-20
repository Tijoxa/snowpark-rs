use crate::{Column, expr::Expr};

/// All columns of the input to a selection, with optional Snowflake modifiers.
#[derive(Clone, Default)]
#[must_use]
pub struct Star {
    pub(crate) pattern: Option<String>,
    pub(crate) exclusions: Vec<String>,
    pub(crate) replacements: Vec<(String, Expr)>,
    pub(crate) renames: Vec<(Expr, String)>,
}

/// Select all columns of the current DataFrame.
pub fn star() -> Star {
    Star::default()
}

impl Star {
    /// Select names matching a Snowflake ILIKE pattern. Incompatible with EXCLUDE.
    ///
    /// Patterns containing a backslash are resolved by Snowflake at execution.
    pub fn ilike(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Omit the given columns from the selection.
    pub fn exclude(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.exclusions.extend(names.into_iter().map(Into::into));
        self
    }

    /// Replace an existing column in place. The expression reads the input columns.
    pub fn replace(mut self, name: impl Into<String>, column: Column) -> Self {
        self.replacements.push((name.into(), column.0));
        self
    }

    /// Rename a selected column in place, after any replacement.
    pub fn rename(mut self, old: impl Into<Column>, new: impl Into<String>) -> Self {
        self.renames.push((old.into().0, new.into()));
        self
    }
}
