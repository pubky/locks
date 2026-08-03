use std::path::PathBuf;

use crate::config::ConfigError;

pub fn parse_config_arg<I>(args: I) -> Result<Option<PathBuf>, ConfigError>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let _program = args.next();
    let mut config_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                if config_path.is_some() {
                    return Err(ConfigError::InvalidArgs(
                        "--config may only be provided once".to_owned(),
                    ));
                }
                let value = args.next().ok_or_else(|| {
                    ConfigError::InvalidArgs("--config requires a path".to_owned())
                })?;
                config_path = Some(PathBuf::from(value));
            }
            "--help" | "-h" => {
                return Err(ConfigError::InvalidArgs(
                    "usage: locks-server [--config <path>]".to_owned(),
                ));
            }
            other => {
                return Err(ConfigError::InvalidArgs(format!(
                    "unknown argument: {other}"
                )));
            }
        }
    }
    Ok(config_path)
}

pub fn home_dir_from_env() -> Result<PathBuf, ConfigError> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or(ConfigError::MissingHome)
}

#[cfg(test)]
mod tests {
    use super::parse_config_arg;

    #[test]
    fn parse_config_arg_accepts_absent_or_explicit_config() {
        assert_eq!(parse_config_arg(["locks-server".to_owned()]).unwrap(), None);
        assert_eq!(
            parse_config_arg([
                "locks-server".to_owned(),
                "--config".to_owned(),
                "/tmp/locks.toml".to_owned(),
            ])
            .unwrap()
            .unwrap()
            .to_str(),
            Some("/tmp/locks.toml")
        );
    }

    #[test]
    fn parse_config_arg_rejects_unknown_or_incomplete_arguments() {
        let missing =
            parse_config_arg(["locks-server".to_owned(), "--config".to_owned()]).unwrap_err();
        assert!(missing.to_string().contains("requires a path"));

        let unknown =
            parse_config_arg(["locks-server".to_owned(), "--port".to_owned()]).unwrap_err();
        assert!(unknown.to_string().contains("unknown argument"));
    }
}
