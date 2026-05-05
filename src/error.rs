use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("invalid compatibility level {level}"))]
    InvalidCompatibilityLevel { level: u16 },

    #[snafu(display("invalid identifier: {reason}"))]
    InvalidIdentifier { reason: String },
}
