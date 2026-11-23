//! The typed edge of the API: the error shape, JSON-message-preserving extractors, and query
//! structs shared across modules. Endpoint-specific request and response types live beside
//! their handlers; everything here is what more than one module needs.
use axum::extract::{FromRequest, FromRequestParts, Query, Request};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Json, Response};
use bytes::Bytes;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub fn err(code: StatusCode, msg: &str) -> Response { (code, Json(ErrMsg { message: msg.into() })).into_response() }

#[derive(Serialize, JsonSchema)]
pub struct ErrMsg { pub message: String }

/// A JSON request body, deserialized into `T`. An absent body reads as `{}`, so required
/// fields report themselves; errors answer in the `ErrMsg` shape.
pub struct Body<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for Body<T> {
    type Rejection = Response;
    async fn from_request(req: Request, state: &S) -> Result<Self, Response> {
        let bytes = Bytes::from_request(req, state).await.map_err(|e| err(e.status(), &e.body_text()))?;
        let bytes = if bytes.is_empty() { Bytes::from_static(b"{}") } else { bytes };
        serde_json::from_slice(&bytes).map(Body).map_err(|e| err(StatusCode::BAD_REQUEST, &format!("invalid body: {e}")))
    }
}

/// The query string, deserialized into `T`; errors answer in the `ErrMsg` shape.
pub struct Qry<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequestParts<S> for Qry<T> {
    type Rejection = Response;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Response> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(v)) => Ok(Qry(v)), Err(e) => Err(err(StatusCode::BAD_REQUEST, &format!("invalid query: {e}")))
        }
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct PathQ { pub path: Option<String> }
