use regex::{Regex, RegexBuilder};

/// Server-side DAG selection, with literal name matching refined locally.
#[derive(Debug, Clone, Default)]
pub struct DagFilter {
    dag_id_pattern: Option<String>,
    name_matcher: Option<Regex>,
    tag: Option<String>,
}

impl DagFilter {
    /// Without `*`, names match a substring. With `*`, match the whole DAG ID.
    /// Name matching is case-insensitive; tags are exact.
    pub fn new(dag_id_pattern: Option<String>, tag: Option<String>) -> Result<Self, regex::Error> {
        let name_matcher = dag_id_pattern
            .as_ref()
            .map(|pattern| {
                let escaped = regex::escape(pattern).replace(r"\*", ".*");
                let expression = if pattern.contains('*') {
                    format!("^{escaped}$")
                } else {
                    escaped
                };
                RegexBuilder::new(&expression)
                    .case_insensitive(true)
                    .build()
            })
            .transpose()?;
        Ok(Self {
            dag_id_pattern,
            name_matcher,
            tag,
        })
    }

    pub(crate) fn query(&self) -> Vec<(&'static str, String)> {
        let mut query = Vec::new();
        if let Some(pattern) = &self.dag_id_pattern {
            // Airflow uses SQL LIKE patterns, not shell wildcards.
            query.push(("dag_id_pattern", pattern.replace('*', "%")));
        }
        if let Some(tag) = &self.tag {
            query.push(("tags", tag.clone()));
        }
        query
    }

    pub(crate) fn matches_name(&self, dag_id: &str) -> bool {
        // Airflow wraps its pattern in %, and treats _ as a wildcard. Refine
        // the server's candidates to honor anchors and literal underscores.
        self.name_matcher
            .as_ref()
            .is_none_or(|matcher| matcher.is_match(dag_id))
    }
}

#[cfg(test)]
mod tests {
    use super::DagFilter;

    #[test]
    fn wildcard_names_are_anchored_and_underscores_are_literal() {
        let filter = DagFilter::new(Some("team_*_daily".into()), None).unwrap();
        assert!(filter.matches_name("TEAM_orders_daily"));
        assert!(filter.matches_name("team__daily"));
        assert!(!filter.matches_name("other_team_orders_daily"));
        assert!(!filter.matches_name("team_orders_daily_extra"));
        assert!(!filter.matches_name("teamXorders_daily"));
    }

    #[test]
    fn plain_names_match_substrings_literally() {
        let filter = DagFilter::new(Some("team.a_".into()), None).unwrap();
        assert!(filter.matches_name("prefix_TEAM.a_orders"));
        assert!(!filter.matches_name("teamXa_orders"));
        assert!(!filter.matches_name("team.aXorders"));
    }

    #[test]
    fn query_combines_name_and_tag_without_changing_tag() {
        let filter =
            DagFilter::new(Some("team_*".into()), Some("daily & important".into())).unwrap();
        assert_eq!(
            filter.query(),
            vec![
                ("dag_id_pattern", "team_%".into()),
                ("tags", "daily & important".into()),
            ]
        );
        let unfiltered = DagFilter::default();
        assert!(unfiltered.query().is_empty());
        assert!(unfiltered.matches_name("anything"));
        let tags_only = DagFilter::new(None, Some("daily".into())).unwrap();
        assert!(tags_only.matches_name("anything"));
    }
}
