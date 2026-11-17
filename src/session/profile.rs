use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use snowflake_connector_rs::{
    AuthConfig, Client, ClientConfig, EndpointConfig, Error, KeyPairConfig, SessionConfig,
};

use super::Session;

/// A configuration or connection error while opening a named Snowflake session.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("Could not find the Snowflake configuration directory")]
    ConfigDirectory,
    #[error("Could not read Snowflake configuration {path}: {source}")]
    ConfigIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Invalid Snowflake configuration in {0}")]
    InvalidConfig(PathBuf),
    #[error("Snowflake connection profile {0:?} was not found")]
    MissingProfile(String),
    #[error("Invalid settings for Snowflake connection profile {0:?}")]
    InvalidProfile(String),
    #[error("Unsupported authentication for Snowflake connection profile {0:?}")]
    UnsupportedAuthentication(String),
    #[error("Could not read credentials for Snowflake connection profile {profile:?} ({kind})")]
    CredentialIo {
        profile: String,
        kind: io::ErrorKind,
    },
    #[error("Snowflake connection failed ({:?}, code {:?})", .0.kind(), .0.snowflake_code())]
    Connect(#[source] Error),
}

#[derive(Default, Deserialize)]
struct CliConfig {
    default_connection_name: Option<String>,
    #[serde(default)]
    connections: toml::Table,
}

#[derive(Deserialize)]
struct Profile {
    account: String,
    user: String,
    authenticator: Option<String>,
    password: Option<String>,
    token: Option<String>,
    token_file_path: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
    private_key_file_pwd: Option<String>,
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    protocol: Option<String>,
}

/// Open a profile from `connections.toml` with the Rust Snowflake connector.
///
/// With no name, use the configured default profile.
pub async fn connect_profile(name: Option<&str>) -> Result<Session, SessionError> {
    let home = dirs::home_dir();
    let snowflake_home = env::var_os("SNOWFLAKE_HOME")
        .map(PathBuf::from)
        .map(|path| expand_home(&path, home.as_deref()))
        .or_else(|| home.as_ref().map(|home| home.join(".snowflake")));
    let directory = snowflake_home
        .filter(|dir| dir.is_dir())
        .or_else(|| dirs::config_local_dir().map(|dir| dir.join("snowflake")))
        .ok_or(SessionError::ConfigDirectory)?;
    let explicit = name
        .map(str::to_owned)
        .or_else(|| env::var("SNOWFLAKE_DEFAULT_CONNECTION_NAME").ok());
    let config = load_profile(&directory, home.as_deref(), explicit)?;
    Client::new(config)
        .map_err(SessionError::Connect)?
        .create_session()
        .await
        .map_err(SessionError::Connect)
}

fn load_profile(
    directory: &Path,
    home: Option<&Path>,
    name: Option<String>,
) -> Result<ClientConfig, SessionError> {
    let connections: Option<toml::Table> = read_config(&directory.join("connections.toml"))?;
    let cli: CliConfig = if name.is_none() || connections.is_none() {
        read_config(&directory.join("config.toml"))?.unwrap_or_default()
    } else {
        CliConfig::default()
    };
    let name = name
        .or(cli.default_connection_name)
        .unwrap_or_else(|| "default".to_owned());
    let mut connections = connections.unwrap_or(cli.connections);
    let profile: Profile = connections
        .remove(&name)
        .ok_or_else(|| SessionError::MissingProfile(name.clone()))?
        .try_into()
        .map_err(|_| SessionError::InvalidProfile(name.clone()))?;
    profile.into_config(&name, directory, home)
}

impl Profile {
    fn into_config(
        self,
        name: &str,
        directory: &Path,
        home: Option<&Path>,
    ) -> Result<ClientConfig, SessionError> {
        let invalid = || SessionError::InvalidProfile(name.to_owned());
        let credential = |path: &Path| {
            let path = directory.join(expand_home(path, home));
            fs::read_to_string(path).map_err(|error| SessionError::CredentialIo {
                profile: name.to_owned(),
                kind: error.kind(),
            })
        };
        let required = |value: Option<String>| {
            value
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(invalid)
        };
        let authenticator = self.authenticator.as_deref().unwrap_or("snowflake");
        let auth = match authenticator.to_ascii_lowercase().as_str() {
            "snowflake" => AuthConfig::password(required(self.password)?),
            "programmatic_access_token" => {
                let token = match self.token_file_path {
                    Some(path) => credential(&path)?.trim().to_owned(),
                    None => required(self.token.or(self.password))?,
                };
                AuthConfig::password(required(Some(token))?)
            }
            "oauth" => AuthConfig::oauth(required(self.token)?),
            "snowflake_jwt" => {
                let pem = credential(&self.private_key_file.ok_or_else(invalid)?)?;
                let key = match self.private_key_file_pwd {
                    Some(password) => KeyPairConfig::from_encrypted_pem(pem, password.into_bytes()),
                    None => KeyPairConfig::from_pem(pem),
                };
                AuthConfig::key_pair(key)
            }
            _ => return Err(SessionError::UnsupportedAuthentication(name.to_owned())),
        };

        let mut context = SessionConfig::new();
        if let Some(warehouse) = self.warehouse {
            context = context.with_warehouse(warehouse);
        }
        if let Some(database) = self.database {
            context = context.with_database(database);
        }
        if let Some(schema) = self.schema {
            context = context.with_schema(schema);
        }
        if let Some(role) = self.role {
            context = context.with_role(role);
        }
        let config = ClientConfig::new(
            required(Some(self.user))?,
            required(Some(self.account))?,
            auth,
        )
        .with_session(context);
        if let Some(host) = self.host {
            let protocol = self.protocol.as_deref().unwrap_or("https");
            let port = self.port.map(|port| format!(":{port}")).unwrap_or_default();
            let endpoint = format!("{protocol}://{host}{port}")
                .parse()
                .map_err(|_| invalid())?;
            Ok(config.with_endpoint(EndpointConfig::custom_base_url(endpoint)))
        } else {
            Ok(config)
        }
    }
}

fn expand_home(path: &Path, home: Option<&Path>) -> PathBuf {
    if let (Ok(relative), Some(home)) = (path.strip_prefix("~"), home) {
        home.join(relative)
    } else {
        path.to_path_buf()
    }
}

fn read_config<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, SessionError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(SessionError::ConfigIo {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    toml::from_str(text.trim_start_matches('\u{feff}'))
        .map(Some)
        .map_err(|_| SessionError::InvalidConfig(path.to_path_buf()))
}
