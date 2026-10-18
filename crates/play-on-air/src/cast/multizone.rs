//! Google Cast multizone (dynamic group) messages: `SET_PLAYBACK_DEVICES`, `GET_STATUS`, and
//! their answers.
//!
//! Wire shapes follow `docs/cast-multizone-spec.md`. A playing Cast session is expanded,
//! shrunk, or moved by sending the complete desired member list to `receiver-0` of the device
//! that hosts the session; the device answers `PLAYBACK_SESSION_UPDATED` (per-member failures in
//! `failedDevices`, a new host in `sessionEndpoint`) or `INVALID_REQUEST`. `GET_STATUS` on a
//! group endpoint answers `MULTIZONE_STATUS` with its member speakers and their volume levels.

use serde::Deserialize;
use serde_json::Value;

use crate::error::{Error, Result};

/// Cast multizone namespace (wire).
pub const MULTIZONE_NAMESPACE: &str = "urn:x-cast:com.google.cast.multizone";

/// One speaker in a `SET_PLAYBACK_DEVICES` member list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackDevice {
  /// Cast device id as the mDNS `id` TXT value announces it.
  pub device_id: String,
  /// The speaker's own Cast address.
  pub ip_address: String,
  /// The speaker's own Cast port (8009 for speakers).
  pub port: u16,
}

/// `SET_PLAYBACK_DEVICES` request body (the worker fills `sessionId` and `requestId`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetPlaybackDevices {
  /// Complete desired member list, session host first.
  pub devices: Vec<PlaybackDevice>,
  /// Optional display name of the group session.
  pub session_name: Option<String>,
}

impl SetPlaybackDevices {
  /// Wire payload for receiver app session `session_id`.
  #[must_use]
  pub fn payload(&self, request_id: u32, session_id: &str) -> Value {
    let devices: Vec<Value> = self
      .devices
      .iter()
      .map(|device| {
        serde_json::json!({
          "deviceId": device.device_id,
          "ipAddress": device.ip_address,
          "port": device.port,
        })
      })
      .collect();
    let mut body = serde_json::json!({
      "requestId": request_id,
      "type": "SET_PLAYBACK_DEVICES",
      "sessionId": session_id,
      "deviceList": devices,
    });
    if let (Some(name), Some(object)) = (&self.session_name, body.as_object_mut()) {
      drop(object.insert("sessionName".to_owned(), Value::String(name.clone())));
    }
    body
  }
}

/// Where the group session lives after a move (`sessionEndpoint`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEndpoint {
  /// Id of the device (or group endpoint) now hosting the session.
  pub device_id: String,
  /// Address of the host.
  pub ip_address: String,
  /// Cast port of the host (a group endpoint uses its own port, not 8009).
  pub port: u16,
  /// Receiver app session id on the host (unchanged by a move).
  pub session_id: Option<String>,
}

/// Successful answer to `SET_PLAYBACK_DEVICES`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackSessionUpdate {
  /// Group session name the device chose.
  pub name: Option<String>,
  /// Members the device could not add, as [`normalize_device_id`] forms.
  pub failed_devices: Vec<String>,
  /// New session host, when the session moved.
  pub endpoint: Option<SessionEndpoint>,
}

/// One member speaker of a group endpoint, from `MULTIZONE_STATUS`.
#[derive(Debug, Clone, PartialEq)]
pub struct MultizoneMember {
  /// Member device id as [`normalize_device_id`] forms it (`MULTIZONE_STATUS` sends it dashed).
  pub device_id: String,
  /// The member's own receiver volume level (`0.0..=1.0`).
  pub level: f32,
}

/// Multizone `GET_STATUS` request body.
#[must_use]
pub fn get_status_payload(request_id: u32) -> Value {
  serde_json::json!({ "requestId": request_id, "type": "GET_STATUS" })
}

#[derive(Deserialize)]
struct StatusBody {
  #[serde(default)]
  devices: Vec<StatusDevice>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatusDevice {
  device_id: String,
  volume: Option<StatusVolume>,
}

#[derive(Deserialize)]
struct StatusVolume {
  level: Option<f32>,
}

/// Parse a multizone message as the `MULTIZONE_STATUS` answer to `request_id`.
///
/// Returns `None` for messages that are not the answer, `Some(Ok)` with the members that carry
/// a volume level, and `Some(Err)` for `INVALID_REQUEST` or a status without a member list.
#[must_use]
pub fn parse_multizone_status_reply(payload: &str, request_id: u32) -> Option<Result<Vec<MultizoneMember>>> {
  let value: Value = serde_json::from_str(payload).ok()?;
  if value.get("requestId").and_then(Value::as_u64) != Some(u64::from(request_id)) {
    return None;
  }
  match value.get("type").and_then(Value::as_str)? {
    "MULTIZONE_STATUS" => Some(
      value
        .get("status")
        .cloned()
        .and_then(|status| serde_json::from_value::<StatusBody>(status).ok())
        .map(|status| {
          status
            .devices
            .into_iter()
            .filter_map(|device| {
              Some(MultizoneMember {
                device_id: normalize_device_id(&device.device_id),
                level: device.volume?.level?,
              })
            })
            .collect()
        })
        .ok_or_else(|| Error::Cast("MULTIZONE_STATUS without a member list".to_owned())),
    ),
    "INVALID_REQUEST" => {
      let reason = value.get("reason").and_then(Value::as_str).unwrap_or("unknown");
      Some(Err(Error::Cast(format!("multizone INVALID_REQUEST: {reason}"))))
    },
    _ => None,
  }
}

/// Device id in the form members are keyed by: lowercase hex without dashes.
#[must_use]
pub fn normalize_device_id(id: &str) -> String {
  id.chars().filter(|c| *c != '-').map(|c| c.to_ascii_lowercase()).collect()
}

/// Parse a multizone message as the answer to `request_id`.
///
/// Returns `None` for messages that are not the answer (other request ids, unsolicited
/// `DEVICE_UPDATED` / `MULTIZONE_STATUS`, unparsable text), `Some(Ok)` for
/// `PLAYBACK_SESSION_UPDATED`, and `Some(Err)` for `INVALID_REQUEST`.
#[must_use]
pub fn parse_set_playback_devices_reply(payload: &str, request_id: u32) -> Option<Result<PlaybackSessionUpdate>> {
  let value: Value = serde_json::from_str(payload).ok()?;
  if value.get("requestId").and_then(Value::as_u64) != Some(u64::from(request_id)) {
    return None;
  }
  match value.get("type").and_then(Value::as_str)? {
    "PLAYBACK_SESSION_UPDATED" => Some(Ok(PlaybackSessionUpdate {
      name: value.get("name").and_then(Value::as_str).map(str::to_owned),
      failed_devices: value
        .get("failedDevices")
        .and_then(Value::as_array)
        .map(|ids| ids.iter().filter_map(Value::as_str).map(normalize_device_id).collect())
        .unwrap_or_default(),
      endpoint: value.get("sessionEndpoint").and_then(parse_endpoint),
    })),
    "INVALID_REQUEST" => {
      let reason = value.get("reason").and_then(Value::as_str).unwrap_or("unknown");
      Some(Err(Error::Cast(format!("multizone INVALID_REQUEST: {reason}"))))
    },
    _ => None,
  }
}

fn parse_endpoint(value: &Value) -> Option<SessionEndpoint> {
  Some(SessionEndpoint {
    device_id: value.get("deviceId").and_then(Value::as_str)?.to_owned(),
    ip_address: value.get("ipAddress").and_then(Value::as_str)?.to_owned(),
    port: value
      .get("port")
      .and_then(Value::as_u64)
      .and_then(|port| u16::try_from(port).ok())?,
    session_id: value.get("sessionId").and_then(Value::as_str).map(str::to_owned),
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  fn two_members() -> SetPlaybackDevices {
    SetPlaybackDevices {
      devices: vec![
        PlaybackDevice {
          device_id: "d417a9b9bf1ebe211639512dad18be1d".to_owned(),
          ip_address: "172.27.10.145".to_owned(),
          port: 8009,
        },
        PlaybackDevice {
          device_id: "56cb7fe7e13f325625325c4a304c57fa".to_owned(),
          ip_address: "172.27.10.213".to_owned(),
          port: 8009,
        },
      ],
      session_name: Some("Pool Bathroom speaker + 1".to_owned()),
    }
  }

  #[test]
  fn set_playback_devices_matches_live_shape() {
    let payload = two_members().payload(7, "0816e28f");
    assert_eq!(
      payload,
      serde_json::json!({
        "requestId": 7,
        "type": "SET_PLAYBACK_DEVICES",
        "sessionId": "0816e28f",
        "deviceList": [
          {"deviceId": "d417a9b9bf1ebe211639512dad18be1d", "ipAddress": "172.27.10.145", "port": 8009},
          {"deviceId": "56cb7fe7e13f325625325c4a304c57fa", "ipAddress": "172.27.10.213", "port": 8009},
        ],
        "sessionName": "Pool Bathroom speaker + 1",
      })
    );
  }

  #[test]
  fn set_playback_devices_omits_missing_name() {
    let mut request = two_members();
    request.session_name = None;
    assert!(request.payload(1, "s").get("sessionName").is_none());
  }

  #[test]
  fn parses_live_playback_session_updated() {
    let live = r#"{"changedDevices":["56cb7fe7e13f325625325c4a304c57fa"],"failedDevices":[],"name":"Pool Bathroom speaker + 1","requestId":7,"sessionEndpoint":{"deviceId":"5c23adbc-893b-446c-bb38-a935d16e1be4","ipAddress":"172.27.10.145","localDeviceId":"5c23adbc-893b-446c-bb38-a935d16e1be4","port":32227,"sessionId":"0816e28f"},"type":"PLAYBACK_SESSION_UPDATED"}"#;
    let update = parse_set_playback_devices_reply(live, 7).expect("answer").expect("ok");
    assert_eq!(update.name.as_deref(), Some("Pool Bathroom speaker + 1"));
    assert_eq!(update.failed_devices, Vec::<String>::new());
    assert_eq!(
      update.endpoint,
      Some(SessionEndpoint {
        device_id: "5c23adbc-893b-446c-bb38-a935d16e1be4".to_owned(),
        ip_address: "172.27.10.145".to_owned(),
        port: 32227,
        session_id: Some("0816e28f".to_owned()),
      })
    );
  }

  #[test]
  fn failed_devices_are_normalized() {
    let reply =
      r#"{"requestId":3,"type":"PLAYBACK_SESSION_UPDATED","failedDevices":["56CB7FE7-E13F-3256-2532-5C4A304C57FA"]}"#;
    let update = parse_set_playback_devices_reply(reply, 3).expect("answer").expect("ok");
    assert_eq!(update.failed_devices, vec!["56cb7fe7e13f325625325c4a304c57fa".to_owned()]);
    assert_eq!(update.endpoint, None);
  }

  #[test]
  fn invalid_request_is_an_error() {
    let reply = r#"{"requestId":4,"type":"INVALID_REQUEST","reason":"INVALID_COMMAND"}"#;
    let err = parse_set_playback_devices_reply(reply, 4)
      .expect("answer")
      .expect_err("refused");
    assert!(err.to_string().contains("INVALID_COMMAND"));
  }

  #[test]
  fn other_messages_are_not_the_answer() {
    assert!(parse_set_playback_devices_reply(r#"{"requestId":5,"type":"PLAYBACK_SESSION_UPDATED"}"#, 6).is_none());
    assert!(parse_set_playback_devices_reply(r#"{"type":"DEVICE_UPDATED","device":{}}"#, 6).is_none());
    assert!(parse_set_playback_devices_reply(r#"{"requestId":6,"type":"MULTIZONE_STATUS"}"#, 6).is_none());
    assert!(parse_set_playback_devices_reply("not json", 6).is_none());
  }

  #[test]
  fn get_status_matches_wire_shape() {
    assert_eq!(
      get_status_payload(901),
      serde_json::json!({"requestId": 901, "type": "GET_STATUS"})
    );
  }

  /// Live answer of the "Gym and Pool" static group endpoint while it hosted nothing.
  #[test]
  fn parses_live_multizone_status() {
    let live = r#"{"requestId":901,"status":{"devices":[{"capabilities":196612,"deviceId":"d417a9b9-bf1e-be21-1639-512dad18be1d","name":"Pool Bathroom speaker","volume":{"level":0.7659151554107666,"muted":false}},{"capabilities":196612,"deviceId":"56cb7fe7-e13f-3256-2532-5c4a304c57fa","name":"Gym speaker","volume":{"level":0.6158643364906311,"muted":false}}]},"type":"MULTIZONE_STATUS"}"#;
    let members = parse_multizone_status_reply(live, 901).expect("answer").expect("ok");
    let ids: Vec<&str> = members.iter().map(|member| member.device_id.as_str()).collect();
    assert_eq!(ids, ["d417a9b9bf1ebe211639512dad18be1d", "56cb7fe7e13f325625325c4a304c57fa"]);
    assert!((members[0].level - 0.765_9).abs() < 1e-4, "{members:?}");
    assert!((members[1].level - 0.615_9).abs() < 1e-4, "{members:?}");
  }

  #[test]
  fn multizone_status_skips_members_without_level() {
    let reply = r#"{"requestId":2,"type":"MULTIZONE_STATUS","status":{"devices":[{"deviceId":"a-b"},{"deviceId":"c-d","volume":{"level":0.5}}]}}"#;
    let members = parse_multizone_status_reply(reply, 2).expect("answer").expect("ok");
    assert_eq!(members, vec![MultizoneMember { device_id: "cd".to_owned(), level: 0.5 }]);
  }

  #[test]
  fn multizone_status_errors_and_non_answers() {
    let refused = r#"{"requestId":3,"type":"INVALID_REQUEST","reason":"INVALID_COMMAND"}"#;
    let err = parse_multizone_status_reply(refused, 3).expect("answer").expect_err("refused");
    assert!(err.to_string().contains("INVALID_COMMAND"));
    let _no_members = parse_multizone_status_reply(r#"{"requestId":3,"type":"MULTIZONE_STATUS"}"#, 3)
      .expect("answer")
      .expect_err("no member list");
    assert!(parse_multizone_status_reply(r#"{"requestId":4,"type":"MULTIZONE_STATUS","status":{}}"#, 3).is_none());
    assert!(parse_multizone_status_reply(r#"{"requestId":3,"type":"PLAYBACK_SESSION_UPDATED"}"#, 3).is_none());
    assert!(parse_multizone_status_reply("not json", 3).is_none());
  }

  #[test]
  fn normalize_strips_dashes_and_case() {
    assert_eq!(
      normalize_device_id("D417A9B9-BF1E-BE21-1639-512DAD18BE1D"),
      "d417a9b9bf1ebe211639512dad18be1d"
    );
  }
}
