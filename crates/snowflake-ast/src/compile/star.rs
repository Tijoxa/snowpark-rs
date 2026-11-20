use std::{
    collections::{HashMap, HashSet},
    fmt::Write as _,
};

use super::{Columns, CompileError, Scope};
use crate::{CompiledQuery, Star, expr::Expr, ident::Ident, plan::LogicalPlan};

impl Scope<'_> {
    pub(super) fn project_star(
        &self,
        star: &Star,
        query: &mut CompiledQuery,
    ) -> Result<Option<Columns>, CompileError> {
        if star.pattern.is_some() && !star.exclusions.is_empty() {
            return Err(CompileError::IncompatibleStarFilters);
        }
        write!(query.sql, "t{}.*", self.left.alias).unwrap();
        if let Some(pattern) = &star.pattern {
            query.sql.push_str(" ILIKE ");
            emit_pattern(&mut query.sql, pattern);
        }

        let mut excluded = HashSet::new();
        if !star.exclusions.is_empty() {
            query.sql.push_str(" EXCLUDE (");
            for (index, name) in star.exclusions.iter().enumerate() {
                add_separator(&mut query.sql, index);
                let (_, name) = self.reference(name, None)?;
                write!(query.sql, "{name}").unwrap();
                if !excluded.insert(name.clone()) {
                    return Err(CompileError::AmbiguousColumn(name.to_string()));
                }
            }
            query.sql.push(')');
        }

        // Let Snowflake resolve patterns that use its backslash escape rules.
        let local_pattern = star
            .pattern
            .as_deref()
            .filter(|pattern| !pattern.contains('\\'));
        let target = |name: &str, relation: Option<&LogicalPlan>| {
            let (_, ident) = self.reference(name, relation)?;
            if excluded.contains(&ident)
                || local_pattern.is_some_and(|pattern| !matches_pattern(ident.as_str(), pattern))
            {
                return Err(CompileError::UnknownColumn(name.to_owned()));
            }
            Ok(ident)
        };
        let mut columns = self.left.columns.cloned();
        if star.pattern.is_some()
            && (local_pattern.is_none()
                || columns
                    .as_ref()
                    .is_some_and(|outputs| outputs.iter().any(|column| column.name.is_none())))
        {
            // An unaliased expression's generated column name is only known to Snowflake.
            columns = None;
        }
        if let Some(columns) = &mut columns {
            columns.retain(|column| {
                column.name.as_ref().is_none_or(|name| {
                    !excluded.contains(name)
                        && local_pattern
                            .is_none_or(|pattern| matches_pattern(name.as_str(), pattern))
                })
            });
        }

        if !star.replacements.is_empty() {
            query.sql.push_str(" REPLACE (");
            let mut replaced = HashSet::new();
            for (index, (name, expr)) in star.replacements.iter().enumerate() {
                add_separator(&mut query.sql, index);
                let name = target(name, None)?;
                if !replaced.insert(name.clone()) {
                    return Err(CompileError::AmbiguousColumn(name.to_string()));
                }
                self.emit_expr(expr, query, false)?;
                write!(query.sql, " AS {name}").unwrap();
                if let Some(column) = columns
                    .iter_mut()
                    .flatten()
                    .find(|column| column.name.as_ref() == Some(&name))
                {
                    (column.data_type, column.nullable) = self.expression_type(expr)?;
                }
            }
            query.sql.push(')');
        }

        if !star.renames.is_empty() {
            let mut renamed = HashMap::new();
            query.sql.push_str(" RENAME (");
            for (index, (expr, new)) in star.renames.iter().enumerate() {
                add_separator(&mut query.sql, index);
                let Expr::Reference { name, relation } = expr else {
                    return Err(CompileError::ExpectedReference);
                };
                let old = target(name, relation.as_deref())?;
                let new = Ident::parse(new)?;
                write!(query.sql, "{old} AS {new}").unwrap();
                if renamed.insert(old.clone(), new).is_some() {
                    return Err(CompileError::AmbiguousColumn(old.to_string()));
                }
            }
            query.sql.push(')');
            if let Some(columns) = &mut columns {
                let mut names = HashSet::with_capacity(columns.len());
                for name in columns.iter_mut().filter_map(|column| column.name.as_mut()) {
                    if let Some(new) = renamed.get(name) {
                        *name = new.clone();
                    }
                    if !names.insert(&*name) {
                        return Err(CompileError::DuplicateOutput(name.to_string()));
                    }
                }
            }
        }
        Ok(columns)
    }
}

fn add_separator(sql: &mut String, index: usize) {
    if index > 0 {
        sql.push_str(", ");
    }
}

fn emit_pattern(sql: &mut String, pattern: &str) {
    // Snowflake requires a literal in SELECT * ILIKE and rejects a bind here.
    sql.push('\'');
    for ch in pattern.chars() {
        match ch {
            '\'' => sql.push_str("''"),
            '\\' => sql.push_str(r"\\"),
            ch if ch.is_control() => write!(sql, "\\u{:04X}", u32::from(ch)).unwrap(),
            ch => sql.push(ch),
        }
    }
    sql.push('\'');
}

fn matches_pattern(name: &str, pattern: &str) -> bool {
    // Each entry records whether the pattern so far matches that prefix of the name.
    let mut matches = vec![false; name.chars().count() + 1];
    matches[0] = true;
    for token in pattern.chars() {
        let mut previous = matches[0];
        matches[0] &= token == '%';
        for (index, ch) in name.chars().enumerate() {
            let next = matches[index + 1];
            matches[index + 1] = match token {
                '%' => matches[index] || next,
                '_' => previous,
                // Snowflake's SELECT modifier folds ASCII case in identifiers.
                token => previous && ch.eq_ignore_ascii_case(&token),
            };
            previous = next;
        }
    }
    matches.last() == Some(&true)
}
