use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Parser;
use crossterm::event::{DisableFocusChange, EnableFocusChange};
use crossterm::ExecutableCommand;
use log::{info, LevelFilter, Log, Metadata, Record};

use crate::app::run_app;
use crate::app::state::App;
use crate::CONFIG_PATHS;
use anyhow::Result;
use flowrs_airflow::managed_services::expand::{expand_managed_services, ManagedServiceConfig};
use flowrs_airflow::client::dag_filter::DagFilter;
use flowrs_config::FlowrsConfig;

struct FileLogger {
    file: Mutex<File>,
    level: LevelFilter,
}

impl Log for FileLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            if let Ok(mut file) = self.file.lock() {
                let _ = writeln!(
                    file,
                    "[{}] {} - {}",
                    record.level(),
                    record.target(),
                    record.args()
                );
            }
        }
    }

    fn flush(&self) {
        if let Ok(mut file) = self.file.lock() {
            let _ = file.flush();
        }
    }
}

#[derive(Parser, Debug)]
pub struct RunCommand {
    #[clap(short, long)]
    pub file: Option<String>,
    /// Only load DAG IDs matching this case-insensitive name or '*' pattern
    #[clap(long, value_name = "PATTERN", value_parser = non_empty_filter)]
    pub dag_id_pattern: Option<String>,
    /// Only load DAGs with this exact tag (combined with the name filter)
    #[clap(long, value_name = "TAG", value_parser = non_empty_filter)]
    pub tag: Option<String>,
}

impl RunCommand {
    pub async fn run(&self) -> Result<()> {
        let dag_filter = DagFilter::new(self.dag_id_pattern.clone(), self.tag.clone())?;
        // setup logging
        if let Ok(log_level) = std::env::var("FLOWRS_LOG") {
            setup_logging(&log_level)?;
        }

        // Read config file
        let path = self.file.as_ref().map(PathBuf::from);
        let mut config = FlowrsConfig::from_file(path.as_ref(), &CONFIG_PATHS)?;
        let ms_config = ManagedServiceConfig {
            services: config.managed_services.clone(),
            gcc_regions: config
                .gcc
                .as_ref()
                .map(|c| c.regions.clone())
                .unwrap_or_default(),
            gcc_projects: config.gcc.as_ref().and_then(|c| c.projects.clone()),
        };
        let (new_servers, errors) = expand_managed_services(ms_config).await?;
        config.extend_servers(new_servers);

        // Generate warnings for legacy config conflict (only when no explicit --file)
        let mut warnings = Vec::new();
        if self.file.is_none() && CONFIG_PATHS.has_legacy_conflict {
            let legacy_path =
                dirs::home_dir().map_or_else(|| PathBuf::from("~/.flowrs"), |h| h.join(".flowrs"));
            warnings.push(format!(
                "Configuration file found in both locations:\n  \
                 - {} (active)\n  \
                 - {} (ignored)\n\n\
                 Consider removing the legacy file.",
                CONFIG_PATHS.write_path.display(),
                legacy_path.display()
            ));
        }

        // Initialize theme before entering raw mode (detection requires terminal queries)
        crate::ui::theme::init_theme(config.theme);

        // setup terminal (includes panic hooks) and run app
        let mut terminal = ratatui::init();
        std::io::stdout().execute(EnableFocusChange)?;

        let mut app = App::new_with_errors_and_warnings(config, errors, warnings);
        app.dag_filter = dag_filter;
        let result = run_app(&mut terminal, Arc::new(Mutex::new(app))).await;

        info!("Shutting down the terminal...");
        std::io::stdout().execute(DisableFocusChange)?;
        ratatui::restore();
        result
    }
}

fn non_empty_filter(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        Err("filter must not be empty".to_string())
    } else {
        Ok(value.to_string())
    }
}

fn setup_logging(log_level: &str) -> Result<()> {
    let log_dir = dirs::state_dir().map_or_else(|| PathBuf::from("."), |p| p.join("flowrs"));
    std::fs::create_dir_all(&log_dir)?;

    let log_file = format!(
        "{}/flowrs-debug-{}.log",
        log_dir.display(),
        chrono::Local::now().format("%Y%m%d%H%M%S")
    );
    let level = match log_level.to_lowercase().as_str() {
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    let logger = FileLogger {
        file: Mutex::new(File::create(log_file)?),
        level,
    };
    log::set_boxed_logger(Box::new(logger))?;
    log::set_max_level(level);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::RunCommand;
    use clap::Parser;

    #[test]
    fn parses_name_and_tag_filters_together() {
        let args =
            RunCommand::try_parse_from(["run", "--dag-id-pattern", "team_*", "--tag", "finance"])
                .unwrap();
        assert_eq!(args.dag_id_pattern.as_deref(), Some("team_*"));
        assert_eq!(args.tag.as_deref(), Some("finance"));
    }

    #[test]
    fn rejects_empty_filters() {
        for flag in ["--dag-id-pattern", "--tag"] {
            assert!(RunCommand::try_parse_from(["run", flag, ""]).is_err());
            assert!(RunCommand::try_parse_from(["run", flag, "   "]).is_err());
        }
    }
}
