use regex_syntax::{
    ast::{AssertionKind, Ast, ClassSet, ClassSetItem, GroupKind, Literal, LiteralKind, parse},
    hir::translate,
};

pub(super) enum PatternError {
    Invalid,
    Unsupported,
}

pub(super) fn snowflake_pattern(pattern: &str) -> Result<(&str, bool, bool), PatternError> {
    let (pattern, case_insensitive) = pattern
        .strip_prefix("(?i)")
        .map_or((pattern, false), |pattern| (pattern, true));
    let ast = parse::Parser::new()
        .parse(pattern)
        .map_err(|_| PatternError::Invalid)?;
    let hir = translate::Translator::new()
        .translate(pattern, &ast)
        .map_err(|_| PatternError::Invalid)?;
    if pattern.is_empty() || compatible(&ast) {
        Ok((
            pattern,
            case_insensitive,
            hir.properties().minimum_len() == Some(0),
        ))
    } else {
        Err(PatternError::Unsupported)
    }
}

fn compatible(ast: &Ast) -> bool {
    match ast {
        Ast::Literal(literal) => compatible_literal(literal),
        Ast::Dot(_) => true,
        Ast::Assertion(assertion) => {
            matches!(
                assertion.kind,
                AssertionKind::StartLine | AssertionKind::EndLine
            )
        }
        Ast::ClassBracketed(class) => {
            matches!(&class.kind, ClassSet::Item(item) if compatible_class(item))
        }
        Ast::Repetition(repetition) => repetition.greedy && compatible(&repetition.ast),
        Ast::Group(group) => {
            matches!(group.kind, GroupKind::CaptureIndex(_))
                && (matches!(*group.ast, Ast::Empty(_)) || compatible(&group.ast))
        }
        Ast::Alternation(alternation) => alternation.asts.iter().all(compatible),
        Ast::Concat(concat) => concat.asts.iter().all(compatible),
        _ => false,
    }
}

fn compatible_literal(literal: &Literal) -> bool {
    matches!(literal.kind, LiteralKind::Verbatim | LiteralKind::Meta) && literal.c != '\0'
}

fn compatible_class(item: &ClassSetItem) -> bool {
    match item {
        ClassSetItem::Literal(literal) => compatible_literal(literal),
        ClassSetItem::Range(range) => {
            range.start.c.is_ascii_alphanumeric()
                && range.end.c.is_ascii_alphanumeric()
                && compatible_literal(&range.start)
                && compatible_literal(&range.end)
        }
        ClassSetItem::Union(union) => union.items.iter().all(compatible_class),
        _ => false,
    }
}
