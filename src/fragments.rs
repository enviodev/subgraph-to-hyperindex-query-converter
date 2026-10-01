//! Type-conditioned fragments on Hyperindex's stand-ins for subgraph interfaces.
//!
//! A subgraph can declare `interface UserTransaction` and let clients write
//!
//! ```graphql
//! userTransactions { id ... on Supply { amount } ... on Borrow { borrowRateMode } }
//! ```
//!
//! Hyperindex has no GraphQL interfaces. The indexer instead emits a concrete
//! `UserTransaction` entity with one nullable single-object link per implementing
//! type (`supply: Supply`, `borrow: Borrow`, ...). Forwarded as-is, Hasura accepts the
//! `... on Supply { }` fragments and silently ignores them, so every row comes back
//! with only the shared fields.
//!
//! This module closes that gap in two halves:
//!
//! 1. **Query**: `... on Supply { sel }` becomes the aliased link field
//!    `_on_Supply: supply { sel }`. The link is found from the schema cache, so no
//!    interface is hard-coded. Named fragments get the same treatment (`...F` where
//!    `fragment F on Supply` becomes `_on_Supply: supply { ...F }`), and fragments
//!    whose type condition equals the surrounding type are flattened.
//! 2. **Response**: a [`ShapePlan`] records where those aliases live, and
//!    [`ShapePlan::apply`] merges each non-null alias object into its parent row and
//!    drops the key. A row ends up exactly as the subgraph would return it: fragment
//!    fields present for the matching type, absent for the others.
//!
//! Anything that cannot be resolved (unknown parent type, no matching link, several
//! matching links) is left untouched, which is the previous behaviour.

use std::collections::{BTreeMap, HashMap, HashSet};

use graphql_parser::query::{
    parse_query, Definition, Field, FragmentDefinition, FragmentSpread, OperationDefinition,
    Selection, SelectionSet, TypeCondition,
};
use serde_json::{Map, Value};

use crate::schema::{self, LinkLookup};

/// Prefix of the response key given to a rewritten link field. A single leading
/// underscore is legal GraphQL (`__` is reserved for introspection).
const ALIAS_PREFIX: &str = "_on_";

/// Where, in a response, rewritten link fields live and must be merged upward.
///
/// Mirrors the shape of the selection set: `children` is keyed by response key
/// (alias, else field name). A child with `hoist` set is a rewritten link whose
/// object is merged into its parent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShapePlan {
    children: BTreeMap<String, ShapePlan>,
    hoist: bool,
}

impl ShapePlan {
    pub fn is_empty(&self) -> bool {
        self.children.is_empty() && !self.hoist
    }

    #[cfg(test)]
    pub fn children_for_test(&self) -> Vec<String> {
        self.children.keys().cloned().collect()
    }

    fn merge(&mut self, other: ShapePlan) {
        self.hoist |= other.hoist;
        for (key, plan) in other.children {
            self.children.entry(key).or_default().merge(plan);
        }
    }

    fn add_child(&mut self, key: String, plan: ShapePlan) {
        if !plan.is_empty() {
            self.children.entry(key).or_default().merge(plan);
        }
    }

    /// Reshape `value` (a response object, or an array of them) in place.
    pub fn apply(&self, value: &mut Value) {
        match value {
            Value::Array(items) => {
                for item in items {
                    self.apply(item);
                }
            }
            Value::Object(obj) => {
                // Children first, so a hoisted link is already in final shape when
                // it is merged into its parent.
                for (key, plan) in &self.children {
                    if let Some(child) = obj.get_mut(key) {
                        plan.apply(child);
                    }
                }
                for (key, plan) in &self.children {
                    if !plan.hoist {
                        continue;
                    }
                    match obj.remove(key) {
                        Some(Value::Object(link)) => merge_into(obj, link),
                        // `null`: this row is some other implementing type.
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}

/// Merge `src` into `dst`. Existing non-null values win; objects merge recursively.
fn merge_into(dst: &mut Map<String, Value>, src: Map<String, Value>) {
    for (key, incoming) in src {
        match dst.get_mut(&key) {
            None => {
                dst.insert(key, incoming);
            }
            Some(Value::Object(existing)) => {
                if let Value::Object(incoming) = incoming {
                    merge_into(existing, incoming);
                }
            }
            Some(existing) if existing.is_null() => *existing = incoming,
            Some(_) => {}
        }
    }
}

/// A root selection after rewriting.
#[derive(Debug, Clone)]
pub struct RootRewrite {
    /// The replacement selection set text, `{ ... }`.
    pub selection: String,
    pub plan: ShapePlan,
}

#[derive(Debug, Default)]
pub struct RewriteOutput {
    /// Replacement for the named-fragment definitions, when any of them changed.
    pub fragments: Option<String>,
    /// One entry per input root, in order; `None` when it needed no rewrite.
    pub roots: Vec<Option<RootRewrite>>,
}

/// Rewrite type-conditioned fragments in each root selection and in the named
/// fragment definitions.
///
/// `roots` pairs the entity type of a root field with its selection set text.
/// Unparseable input is passed through untouched rather than failing the request.
pub fn rewrite_all<'a>(fragments_text: &'a str, roots: &[(&'a str, &'a str)]) -> RewriteOutput {
    let mut rewriter = Rewriter::new(fragments_text);
    rewriter.plan_all_definitions();
    let roots = roots
        .iter()
        .map(|(entity, selection)| rewriter.rewrite_root(selection, entity))
        .collect();
    RewriteOutput {
        fragments: rewriter.rewritten_definitions(),
        roots,
    }
}

type Set<'a> = SelectionSet<'a, String>;

struct PlannedDefinition<'a> {
    set: Set<'a>,
    plan: ShapePlan,
    changed: bool,
}

struct Rewriter<'a> {
    definitions: HashMap<String, FragmentDefinition<'a, String>>,
    order: Vec<String>,
    planned: HashMap<String, PlannedDefinition<'a>>,
    in_progress: HashSet<String>,
    /// Count of rewrites made so far; compared before and after to detect change.
    edits: usize,
}

impl<'a> Rewriter<'a> {
    fn new(fragments_text: &'a str) -> Self {
        let mut definitions = HashMap::new();
        let mut order = Vec::new();
        if !fragments_text.trim().is_empty() {
            match parse_query::<String>(fragments_text) {
                Ok(doc) => {
                    for def in doc.definitions {
                        if let Definition::Fragment(f) = def {
                            order.push(f.name.clone());
                            definitions.insert(f.name.clone(), f);
                        }
                    }
                }
                Err(e) => tracing::warn!("Could not parse fragment definitions: {}", e),
            }
        }
        Rewriter {
            definitions,
            order,
            planned: HashMap::new(),
            in_progress: HashSet::new(),
            edits: 0,
        }
    }

    fn plan_all_definitions(&mut self) {
        for name in self.order.clone() {
            self.plan_definition(&name);
        }
    }

    /// Rewrite a named fragment against its own type condition, once.
    fn plan_definition(&mut self, name: &str) -> ShapePlan {
        if let Some(done) = self.planned.get(name) {
            return done.plan.clone();
        }
        // A fragment that spreads itself is invalid GraphQL; do not recurse forever.
        if !self.in_progress.insert(name.to_string()) {
            return ShapePlan::default();
        }
        let Some(def) = self.definitions.get(name).cloned() else {
            self.in_progress.remove(name);
            return ShapePlan::default();
        };
        let TypeCondition::On(type_name) = &def.type_condition;
        let before = self.edits;
        let mut set = def.selection_set.clone();
        let plan = self.rewrite_set(&mut set, Some(type_name));
        let changed = self.edits != before;
        self.in_progress.remove(name);
        self.planned.insert(
            name.to_string(),
            PlannedDefinition {
                set,
                plan: plan.clone(),
                changed,
            },
        );
        plan
    }

    fn rewritten_definitions(&self) -> Option<String> {
        if !self.planned.values().any(|p| p.changed) {
            return None;
        }
        let printed: Vec<String> = self
            .order
            .iter()
            .filter_map(|name| {
                let mut def = self.definitions.get(name)?.clone();
                if let Some(planned) = self.planned.get(name) {
                    def.selection_set = planned.set.clone();
                }
                Some(def.to_string())
            })
            .collect();
        Some(printed.join("\n"))
    }

    fn rewrite_root(&mut self, selection: &'a str, entity: &str) -> Option<RootRewrite> {
        // Nothing to do unless a fragment is present; skips parsing for most queries.
        if !selection.contains("...") {
            return None;
        }
        let doc = parse_query::<String>(selection).ok()?;
        let mut definitions = doc.definitions.into_iter();
        let (Some(Definition::Operation(OperationDefinition::SelectionSet(mut set))), None) =
            (definitions.next(), definitions.next())
        else {
            return None;
        };
        let before = self.edits;
        let plan = self.rewrite_set(&mut set, Some(entity));
        // A root that only spreads a rewritten named fragment is textually unchanged
        // but still needs the fragment's plan applied to its response.
        if self.edits == before && plan.is_empty() {
            return None;
        }
        Some(RootRewrite {
            selection: set.to_string(),
            plan,
        })
    }

    /// Rewrite `set`, whose enclosing type is `parent` when known.
    fn rewrite_set(&mut self, set: &mut Set<'a>, parent: Option<&str>) -> ShapePlan {
        let mut plan = ShapePlan::default();
        let items = std::mem::take(&mut set.items);
        let mut out: Vec<Selection<'a, String>> = Vec::with_capacity(items.len());

        for item in items {
            match item {
                Selection::Field(mut field) => {
                    if !field.selection_set.items.is_empty() {
                        let child_type =
                            parent.and_then(|p| schema::nested_type_of(p, &field.name));
                        let child_plan =
                            self.rewrite_set(&mut field.selection_set, child_type.as_deref());
                        let key = field.alias.clone().unwrap_or_else(|| field.name.clone());
                        plan.add_child(key, child_plan);
                    }
                    out.push(Selection::Field(field));
                }
                Selection::InlineFragment(mut inline) => {
                    let target = inline
                        .type_condition
                        .as_ref()
                        .map(|TypeCondition::On(t)| t.clone());

                    // `... on Same { }` or `... { }`: its fields already belong here.
                    let applies_here = target.is_none() || target.as_deref() == parent;
                    if applies_here && inline.directives.is_empty() {
                        plan.merge(self.rewrite_set(&mut inline.selection_set, parent));
                        out.extend(inline.selection_set.items);
                        self.edits += 1;
                        continue;
                    }

                    match (target.as_deref(), parent) {
                        (Some(t), Some(p)) if t != p => match schema::link_field_for_type(p, t) {
                            LinkLookup::One(link) => {
                                let mut inner = self.rewrite_set(&mut inline.selection_set, Some(t));
                                inner.hoist = true;
                                let key = format!("{ALIAS_PREFIX}{t}");
                                plan.add_child(key.clone(), inner);
                                out.push(Selection::Field(Field {
                                    position: inline.position,
                                    alias: Some(key),
                                    name: link,
                                    arguments: Vec::new(),
                                    directives: inline.directives,
                                    selection_set: inline.selection_set,
                                }));
                                self.edits += 1;
                            }
                            other => {
                                tracing::warn!(
                                    "Leaving `... on {}` under {} unchanged: link lookup was {:?}",
                                    t,
                                    p,
                                    other
                                );
                                out.push(Selection::InlineFragment(inline));
                            }
                        },
                        _ => {
                            // Parent unknown, or same-type with directives: keep the
                            // fragment, but fields inside it still sit at this level.
                            let inside = target.as_deref().or(parent);
                            plan.merge(self.rewrite_set(&mut inline.selection_set, inside));
                            out.push(Selection::InlineFragment(inline));
                        }
                    }
                }
                Selection::FragmentSpread(spread) => {
                    let name = spread.fragment_name.clone();
                    let def_type = self.definitions.get(&name).map(|d| {
                        let TypeCondition::On(t) = &d.type_condition;
                        t.clone()
                    });
                    let Some(def_type) = def_type else {
                        // Defined somewhere we cannot see; leave it.
                        out.push(Selection::FragmentSpread(spread));
                        continue;
                    };
                    let fragment_plan = self.plan_definition(&name);

                    match parent {
                        Some(p) if p != def_type => {
                            match schema::link_field_for_type(p, &def_type) {
                                LinkLookup::One(link) => {
                                    let mut inner = fragment_plan;
                                    inner.hoist = true;
                                    let key = format!("{ALIAS_PREFIX}{def_type}");
                                    plan.add_child(key.clone(), inner);
                                    let position = spread.position;
                                    out.push(Selection::Field(Field {
                                        position,
                                        alias: Some(key),
                                        name: link,
                                        arguments: Vec::new(),
                                        directives: spread.directives,
                                        selection_set: SelectionSet {
                                            span: (position, position),
                                            items: vec![Selection::FragmentSpread(
                                                FragmentSpread {
                                                    position,
                                                    fragment_name: name,
                                                    directives: Vec::new(),
                                                },
                                            )],
                                        },
                                    }));
                                    self.edits += 1;
                                }
                                other => {
                                    tracing::warn!(
                                        "Leaving `...{}` (on {}) under {} unchanged: link lookup was {:?}",
                                        name,
                                        def_type,
                                        p,
                                        other
                                    );
                                    out.push(Selection::FragmentSpread(spread));
                                }
                            }
                        }
                        _ => {
                            plan.merge(fragment_plan);
                            out.push(Selection::FragmentSpread(spread));
                        }
                    }
                }
            }
        }

        set.items = out;
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn squash(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn root(entity: &str, selection: &str) -> RootRewrite {
        crate::schema::init_test_schema_once();
        let out = rewrite_all("", &[(entity, selection)]);
        out.roots.into_iter().next().unwrap().expect("expected a rewrite")
    }

    fn untouched(entity: &str, selection: &str) {
        crate::schema::init_test_schema_once();
        let out = rewrite_all("", &[(entity, selection)]);
        assert!(out.roots[0].is_none(), "expected no rewrite for {selection}");
    }

    #[test]
    fn inline_fragment_becomes_aliased_link_field() {
        let r = root("UserTransaction", "{ id timestamp ... on Supply { amount assetPriceUSD } }");
        assert_eq!(
            squash(&r.selection),
            "{ id timestamp _on_Supply: supply { amount assetPriceUSD } }"
        );
        assert!(r.plan.children["_on_Supply"].hoist);
    }

    #[test]
    fn every_implementing_type_gets_its_own_link() {
        let r = root(
            "UserTransaction",
            "{ id ... on Supply { amount } ... on Borrow { borrowRateMode } ... on Repay { amount } }",
        );
        let sel = squash(&r.selection);
        assert!(sel.contains("_on_Supply: supply { amount }"), "{sel}");
        assert!(sel.contains("_on_Borrow: borrow { borrowRateMode }"), "{sel}");
        assert!(sel.contains("_on_Repay: repay { amount }"), "{sel}");
        assert_eq!(r.plan.children.len(), 3);
    }

    #[test]
    fn nested_selection_inside_a_fragment_is_kept() {
        let r = root(
            "UserTransaction",
            "{ id ... on Supply { amount reserve { symbol decimals } } }",
        );
        assert_eq!(
            squash(&r.selection),
            "{ id _on_Supply: supply { amount reserve { symbol decimals } } }"
        );
    }

    #[test]
    fn same_type_fragment_is_flattened() {
        let r = root("UserTransaction", "{ id ... on UserTransaction { txHash } }");
        assert_eq!(squash(&r.selection), "{ id txHash }");
        assert!(r.plan.is_empty());
    }

    #[test]
    fn fragment_without_type_condition_is_flattened() {
        let r = root("UserTransaction", "{ id ... { txHash } }");
        assert_eq!(squash(&r.selection), "{ id txHash }");
    }

    #[test]
    fn directives_on_the_fragment_move_to_the_link() {
        let r = root("UserTransaction", "{ id ... on Supply @include(if: $withSupply) { amount } }");
        assert_eq!(
            squash(&r.selection),
            "{ id _on_Supply: supply @include(if: $withSupply) { amount } }"
        );
    }

    #[test]
    fn repeated_fragments_on_one_type_share_an_alias() {
        let r = root(
            "UserTransaction",
            "{ id ... on Supply { amount } ... on Supply { assetPriceUSD } }",
        );
        assert_eq!(r.plan.children.len(), 1);
        let sel = squash(&r.selection);
        assert_eq!(sel.matches("_on_Supply: supply").count(), 2, "{sel}");
    }

    #[test]
    fn fragments_inside_a_nested_list_are_rewritten() {
        let r = root("TxUser", "{ id userTransactions { id ... on Supply { amount } } }");
        let list = &r.plan.children["userTransactions"];
        assert!(list.children["_on_Supply"].hoist);
        assert!(squash(&r.selection).contains("_on_Supply: supply { amount }"));
    }

    #[test]
    fn aliased_field_keys_the_plan_by_alias() {
        let r = root("TxUser", "{ recent: userTransactions { ... on Supply { amount } } }");
        assert!(r.plan.children.contains_key("recent"));
        assert!(!r.plan.children.contains_key("userTransactions"));
    }

    #[test]
    fn unknown_or_unlinked_types_are_left_alone() {
        // No Supply link on Reserve.
        untouched("Reserve", "{ id ... on Supply { amount } }");
        // Not in the schema at all.
        untouched("Nope", "{ id ... on Supply { amount } }");
        // Type with no link from the parent.
        untouched("UserTransaction", "{ id ... on Swap { amount } }");
    }

    #[test]
    fn ambiguous_link_is_left_alone() {
        // TxAccount has two single-object fields of type Supply.
        untouched("TxAccount", "{ id ... on Supply { amount } }");
    }

    #[test]
    fn list_fields_are_never_used_as_a_link() {
        // TxAccount.txs is [UserTransaction!]!, which is not a one-record link.
        untouched("TxAccount", "{ id ... on UserTransaction { id } }");
    }

    #[test]
    fn selection_without_fragments_is_not_parsed_or_changed() {
        untouched("UserTransaction", "{ id timestamp supply { amount } }");
    }

    #[test]
    fn unparseable_selection_is_passed_through() {
        untouched("UserTransaction", "{ id ... on Supply { amount ");
    }

    #[test]
    fn named_fragment_on_a_linked_type_is_spread_through_the_link() {
        crate::schema::init_test_schema_once();
        let out = rewrite_all(
            "fragment SupplyFields on Supply { amount }",
            &[("UserTransaction", "{ id ...SupplyFields }")],
        );
        assert!(out.fragments.is_none(), "definition itself is unchanged");
        let r = out.roots[0].clone().unwrap();
        assert_eq!(squash(&r.selection), "{ id _on_Supply: supply { ...SupplyFields } }");
        assert!(r.plan.children["_on_Supply"].hoist);
    }

    #[test]
    fn named_fragment_on_the_parent_type_with_inner_fragments_is_rewritten() {
        crate::schema::init_test_schema_once();
        let out = rewrite_all(
            "fragment Tx on UserTransaction { id ... on Supply { amount } }",
            &[("UserTransaction", "{ ...Tx }")],
        );
        let frag = squash(&out.fragments.expect("definition should be rewritten"));
        assert!(frag.contains("_on_Supply: supply { amount }"), "{frag}");
        // The root text is unchanged, but its response still needs reshaping.
        let r = out.roots[0].clone().expect("plan must be carried to the spread site");
        assert!(r.plan.children["_on_Supply"].hoist);
    }

    #[test]
    fn nested_named_fragments_compose() {
        crate::schema::init_test_schema_once();
        let out = rewrite_all(
            "fragment Inner on Supply { amount reserve { symbol } }\n\
             fragment Outer on UserTransaction { id ...Inner }",
            &[("UserTransaction", "{ ...Outer }")],
        );
        let frag = squash(&out.fragments.unwrap());
        assert!(frag.contains("_on_Supply: supply { ...Inner }"), "{frag}");
        assert!(out.roots[0].as_ref().unwrap().plan.children["_on_Supply"].hoist);
    }

    #[test]
    fn self_referencing_fragment_terminates() {
        crate::schema::init_test_schema_once();
        let out = rewrite_all(
            "fragment Loop on UserTransaction { id ...Loop }",
            &[("UserTransaction", "{ ...Loop }")],
        );
        assert!(out.roots[0].is_none());
    }

    #[test]
    fn unknown_fragment_spread_is_kept() {
        untouched("UserTransaction", "{ id ...Missing }");
    }

    // ---- response reshaping ---------------------------------------------------

    fn plan(entity: &str, selection: &str) -> ShapePlan {
        root(entity, selection).plan
    }

    #[test]
    fn rows_keep_only_the_fragment_that_matches_them() {
        let p = plan(
            "UserTransaction",
            "{ id ... on Supply { amount } ... on Borrow { borrowRateMode } }",
        );
        let mut data = json!([
            {"id": "1", "_on_Supply": {"amount": "5"}, "_on_Borrow": null},
            {"id": "2", "_on_Supply": null, "_on_Borrow": {"borrowRateMode": 2}},
        ]);
        p.apply(&mut data);
        assert_eq!(
            data,
            json!([{"id": "1", "amount": "5"}, {"id": "2", "borrowRateMode": 2}])
        );
    }

    #[test]
    fn a_row_matching_no_fragment_has_no_stray_keys() {
        let p = plan("UserTransaction", "{ id ... on Supply { amount } }");
        let mut data = json!([{"id": "1", "_on_Supply": null}]);
        p.apply(&mut data);
        assert_eq!(data, json!([{"id": "1"}]));
    }

    #[test]
    fn nested_objects_inside_a_fragment_come_up_with_it() {
        let p = plan("UserTransaction", "{ id ... on Supply { reserve { symbol } } }");
        let mut data = json!([{"id": "1", "_on_Supply": {"reserve": {"symbol": "WSOMI"}}}]);
        p.apply(&mut data);
        assert_eq!(data, json!([{"id": "1", "reserve": {"symbol": "WSOMI"}}]));
    }

    #[test]
    fn rows_in_a_nested_list_are_reshaped() {
        let p = plan("TxUser", "{ id userTransactions { id ... on Supply { amount } } }");
        let mut data = json!([{"id": "u", "userTransactions": [
            {"id": "1", "_on_Supply": {"amount": "5"}},
            {"id": "2", "_on_Supply": null},
        ]}]);
        p.apply(&mut data);
        assert_eq!(
            data,
            json!([{"id": "u", "userTransactions": [{"id": "1", "amount": "5"}, {"id": "2"}]}])
        );
    }

    #[test]
    fn an_object_root_for_a_by_pk_query_is_reshaped() {
        let p = plan("UserTransaction", "{ id ... on Supply { amount } }");
        let mut data = json!({"id": "1", "_on_Supply": {"amount": "5"}});
        p.apply(&mut data);
        assert_eq!(data, json!({"id": "1", "amount": "5"}));
    }

    #[test]
    fn a_null_root_is_left_null() {
        let p = plan("UserTransaction", "{ id ... on Supply { amount } }");
        let mut data = Value::Null;
        p.apply(&mut data);
        assert_eq!(data, Value::Null);
    }

    #[test]
    fn existing_values_win_over_hoisted_ones() {
        let p = plan("UserTransaction", "{ id ... on Supply { id amount } }");
        let mut data = json!([{"id": "1", "_on_Supply": {"id": "other", "amount": "5"}}]);
        p.apply(&mut data);
        assert_eq!(data, json!([{"id": "1", "amount": "5"}]));
    }

    #[test]
    fn repeated_fragments_merge_their_fields_into_the_row() {
        let p = plan(
            "UserTransaction",
            "{ id ... on Supply { amount } ... on Supply { assetPriceUSD } }",
        );
        let mut data = json!([{"id": "1", "_on_Supply": {"amount": "5", "assetPriceUSD": "0"}}]);
        p.apply(&mut data);
        assert_eq!(data, json!([{"id": "1", "amount": "5", "assetPriceUSD": "0"}]));
    }
}
