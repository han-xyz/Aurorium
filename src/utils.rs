use std::{
    net::SocketAddr,
    path::{Component, Path},
};

use axum::{
    extract::{ConnectInfo, FromRequestParts},
    http::request::Parts,
};
use reqwest::StatusCode;

#[must_use]
pub fn is_safe_relative(name: &str) -> bool {
    !name.is_empty()
        && !name.contains(['\0', '\\'])
        && Path::new(name)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}

pub enum Endianness {
    Little,
    Big,
}

#[must_use]
pub fn hex_decode(hex_string: &str, endianness: &Endianness) -> Option<Vec<u8>> {
    if !hex_string.len().is_multiple_of(2) {
        return None;
    }

    let bytes: Option<Vec<u8>> = (0..hex_string.len())
        .step_by(2)
        .map(|i| {
            let byte = u8::from_str_radix(&hex_string[i..i + 2], 16).ok()?;
            match endianness {
                Endianness::Little => Some(byte),
                Endianness::Big => Some(byte.reverse_bits()),
            }
        })
        .collect();

    bytes
}

#[derive(Debug)]
pub struct ConnectionAddr(pub String);

impl<S> FromRequestParts<S> for ConnectionAddr
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // First try to get X-Forwarded-For header
        if let Some(forwarded_for) = parts
            .headers
            .get("X-Forwarded-For")
            .and_then(|value| value.to_str().ok())
        {
            return Ok(ConnectionAddr(forwarded_for.to_string()));
        }

        // If header not found, fall back to connection info
        let connection_info = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(ConnectionAddr(connection_info.0.ip().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::is_safe_relative;

    #[test]
    fn safe_relative_matrix() {
        for name in ["V_r803238.Wizard_1_610", "Data/GameData/Root.wad", "a.b"] {
            assert!(is_safe_relative(name), "{name:?} should be accepted");
        }
        for name in [
            "",
            "..",
            "../x",
            "a/../x",
            "/etc/passwd",
            "C:\\x",
            "a\\b",
            "a\0b",
            ".",
            "./x",
        ] {
            assert!(!is_safe_relative(name), "{name:?} should be rejected");
        }
    }
}
