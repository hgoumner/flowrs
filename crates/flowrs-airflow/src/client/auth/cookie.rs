use std::fmt;

use async_trait::async_trait;
use log::info;
use reqwest::header::COOKIE;
use reqwest::RequestBuilder;

use super::command::CachedCommand;
use super::AuthProvider;
use crate::auth::CookieSource;
use crate::error::Result;

/// Cookie name Airflow 2.x (Flask) uses for the web UI session; a bare value is
/// sent under this name.
const DEFAULT_COOKIE_NAME: &str = "session";

/// Authenticates by sending a browser session cookie, for Airflow instances
/// whose web UI sits behind SSO and that accept the session on the REST API.
pub struct CookieProvider {
    source: Source,
}

enum Source {
    Static(String),
    Command(CachedCommand),
}

impl CookieProvider {
    pub fn new(source: &CookieSource) -> Self {
        let source = match source {
            CookieSource::Static { cookie } => Source::Static(cookie_header(cookie)),
            CookieSource::Command { cmd } => {
                Source::Command(CachedCommand::new(cmd.clone(), "Cookie"))
            }
        };
        Self { source }
    }
}

impl fmt::Debug for CookieProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("CookieProvider");
        match &self.source {
            Source::Static(_) => s.field("cookie", &"***redacted***"),
            Source::Command(command) => s.field("cmd", &command.cmd()),
        };
        s.finish()
    }
}

#[async_trait]
impl AuthProvider for CookieProvider {
    async fn authenticate(&self, request: RequestBuilder) -> Result<RequestBuilder> {
        let header = match &self.source {
            Source::Static(header) => {
                info!("🔑 Cookie Auth (static)");
                header.clone()
            }
            Source::Command(command) => cookie_header(&command.get("cookie command").await?),
        };
        Ok(request.header(COOKIE, header))
    }
}

/// Turn user input into a `Cookie` header value.
///
/// Accepts a bare cookie value (sent as `session=<value>`), a `name=value; ...`
/// cookie string, or either of those prefixed with `Cookie:` as copied from the
/// browser's network tab.
pub(crate) fn cookie_header(raw: &str) -> String {
    let mut value = raw.trim().trim_matches('"').trim();
    if value
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cookie:"))
    {
        value = value[7..].trim();
    }
    if is_cookie_pair_list(value) {
        value.to_string()
    } else {
        format!("{DEFAULT_COOKIE_NAME}={value}")
    }
}

/// Whether `value` looks like `name=value` pairs rather than a bare value. A
/// bare value may itself contain `=` (base64 padding), so the part before the
/// first `=` must be a valid cookie name and must be followed by a value.
fn is_cookie_pair_list(value: &str) -> bool {
    let Some((name, rest)) = value.split_once('=') else {
        return false;
    };
    let is_token_char = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c);
    !name.is_empty()
        && name.chars().all(is_token_char)
        && !rest.is_empty()
        && !rest.starts_with('=')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookie(request: RequestBuilder) -> String {
        request
            .build()
            .unwrap()
            .headers()
            .get(COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    fn get() -> RequestBuilder {
        reqwest::Client::new().get("http://localhost:8080/api/v1/dags")
    }

    #[test]
    fn bare_value_uses_session_cookie_name() {
        assert_eq!(cookie_header("abc.def-ghi"), "session=abc.def-ghi");
        assert_eq!(cookie_header("  \"abc\"\n"), "session=abc");
    }

    #[test]
    fn bare_value_with_base64_padding_is_not_a_pair() {
        assert_eq!(cookie_header("YWJj=="), "session=YWJj==");
    }

    #[test]
    fn name_value_pairs_are_kept() {
        assert_eq!(cookie_header("session=abc"), "session=abc");
        assert_eq!(
            cookie_header("session=abc; _oauth2_proxy=xyz"),
            "session=abc; _oauth2_proxy=xyz"
        );
    }

    #[test]
    fn cookie_prefix_is_stripped() {
        assert_eq!(cookie_header("Cookie: session=abc"), "session=abc");
        assert_eq!(cookie_header("cookie:abc"), "session=abc");
    }

    #[tokio::test]
    async fn static_cookie_sets_cookie_header() {
        let provider = CookieProvider::new(&CookieSource::Static {
            cookie: "my-session".to_string(),
        });
        let request = provider.authenticate(get()).await.unwrap();
        assert_eq!(cookie(request), "session=my-session");
    }

    #[tokio::test]
    async fn static_cookie_does_not_set_authorization() {
        let provider = CookieProvider::new(&CookieSource::Static {
            cookie: "my-session".to_string(),
        });
        let built = provider.authenticate(get()).await.unwrap().build().unwrap();
        assert!(built.headers().get("authorization").is_none());
    }

    #[tokio::test]
    async fn command_cookie_sets_cookie_header() {
        let provider = CookieProvider::new(&CookieSource::Command {
            cmd: "echo 'session=from-cmd; other=1'".to_string(),
        });
        let request = provider.authenticate(get()).await.unwrap();
        assert_eq!(cookie(request), "session=from-cmd; other=1");
    }

    #[tokio::test]
    async fn command_cookie_failure_is_an_error() {
        let provider = CookieProvider::new(&CookieSource::Command {
            cmd: "false".to_string(),
        });
        let error = provider.authenticate(get()).await.unwrap_err();
        assert!(error.to_string().contains("Cookie helper command failed"));
    }

    #[test]
    fn debug_redacts_static_cookie() {
        let provider = CookieProvider::new(&CookieSource::Static {
            cookie: "secret-value".to_string(),
        });
        assert!(!format!("{provider:?}").contains("secret-value"));
    }
}
