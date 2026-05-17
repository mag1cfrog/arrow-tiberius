use std::env;
use std::error::Error;

use arrow_odbc::odbc_api::Environment;

const CONNECTION_STRING_ENV: &str = "ARROW_TIBERIUS_BENCH_ODBC_CONNECTION_STRING";
const DATABASE_ENV: &str = "ARROW_TIBERIUS_BENCH_DATABASE";

fn main() -> Result<(), Box<dyn Error>> {
    let command = env::args().nth(1);

    match command.as_deref() {
        Some("validate") => validate(),
        Some(command) => Err(format!("unknown arrow-odbc runner command `{command}`").into()),
        None => Err("missing arrow-odbc runner command".into()),
    }
}

fn validate() -> Result<(), Box<dyn Error>> {
    let connection_string = required_env(CONNECTION_STRING_ENV)?;
    let database = required_env(DATABASE_ENV)?;
    let environment = Environment::new()?;
    let _connection =
        environment.connect_with_connection_string(&connection_string, Default::default())?;

    println!("arrow-odbc runner validated database {database}");
    Ok(())
}

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    let value =
        env::var(name).map_err(|_| format!("missing required environment variable {name}"))?;

    if value.is_empty() {
        return Err(format!("required environment variable {name} is empty").into());
    }

    Ok(value)
}
