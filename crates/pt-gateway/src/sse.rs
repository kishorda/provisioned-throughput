//! Server-sent event inspection for streamed completions.
//!
//! The gateway forwards the engine's events unchanged, while recording when content starts
//! and stops (for TTFT and TPOT) and capturing the final usage chunk (for settlement).

use std::time::Instant;

use bytes::Bytes;
use serde_json::Value;

/// Token counts reported by the engine.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct EngineUsage {
    pub prompt_tokens: u64,
    pub cached_tokens: u64,
    pub completion_tokens: u64,
}

impl EngineUsage {
    pub fn from_json(v: &Value) -> Option<Self> {
        let u = v.get("usage").filter(|u| !u.is_null())?;
        Some(Self {
            prompt_tokens: u.get("prompt_tokens")?.as_u64()?,
            cached_tokens: u
                .pointer("/prompt_tokens_details/cached_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            completion_tokens: u.get("completion_tokens")?.as_u64()?,
        })
    }
}

/// What the gateway has seen of the response so far.
#[derive(Debug, Clone, Default)]
pub struct Observations {
    pub usage: Option<EngineUsage>,
    pub first_content_at: Option<Instant>,
    pub last_content_at: Option<Instant>,
    /// Content chunks seen. Approximates output tokens if the stream ends without usage.
    pub content_chunks: u64,
}

/// Splits a byte stream into SSE events and inspects each one.
pub struct SseInspector {
    buf: Vec<u8>,
    /// The gateway always asks the engine for usage. If the client didn't, drop the
    /// usage-only chunk so the client sees what it asked for.
    strip_usage_chunk: bool,
}

impl SseInspector {
    pub fn new(strip_usage_chunk: bool) -> Self {
        Self {
            buf: Vec::new(),
            strip_usage_chunk,
        }
    }

    /// Feed bytes from the engine. Returns the bytes to forward to the client.
    pub fn push(&mut self, chunk: &[u8], obs: &mut Observations, now: Instant) -> Bytes {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::with_capacity(self.buf.len());
        while let Some(end) = find_event_end(&self.buf) {
            let event: Vec<u8> = self.buf.drain(..end).collect();
            if self.inspect(&event, obs, now) {
                out.extend_from_slice(&event);
            }
        }
        Bytes::from(out)
    }

    /// Flush any trailing partial event at end of stream.
    pub fn finish(&mut self) -> Bytes {
        Bytes::from(std::mem::take(&mut self.buf))
    }

    /// Returns whether to forward the event.
    fn inspect(&self, event: &[u8], obs: &mut Observations, now: Instant) -> bool {
        let Ok(text) = std::str::from_utf8(event) else {
            return true;
        };
        let data: String = text
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(str::trim_start)
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() || data == "[DONE]" {
            return true;
        }
        let Ok(v) = serde_json::from_str::<Value>(&data) else {
            return true;
        };

        let has_content = v
            .get("choices")
            .and_then(Value::as_array)
            .is_some_and(|choices| {
                choices.iter().any(|c| {
                    c.pointer("/delta/content")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty())
                })
            });
        if has_content {
            obs.content_chunks += 1;
            obs.first_content_at.get_or_insert(now);
            obs.last_content_at = Some(now);
        }

        if let Some(usage) = EngineUsage::from_json(&v) {
            obs.usage = Some(usage);
            let usage_only = v
                .get("choices")
                .and_then(Value::as_array)
                .is_none_or(|c| c.is_empty());
            if usage_only && self.strip_usage_chunk {
                return false;
            }
        }
        true
    }
}

/// Index just past the blank line that ends the first complete event, if any.
fn find_event_end(buf: &[u8]) -> Option<usize> {
    let lf = buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2);
    let crlf = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4);
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONTENT: &[u8] =
        b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"tok \"}}]}\n\n";
    const USAGE: &[u8] = b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\n";
    const DONE: &[u8] = b"data: [DONE]\n\n";

    #[test]
    fn records_content_and_usage_across_split_chunks() {
        let mut inspector = SseInspector::new(false);
        let mut obs = Observations::default();
        let now = Instant::now();
        let all = [CONTENT, CONTENT, USAGE, DONE].concat();
        let (a, b) = all.split_at(37); // split mid-event
        let mut out = inspector.push(a, &mut obs, now).to_vec();
        out.extend_from_slice(&inspector.push(b, &mut obs, now));
        assert_eq!(out, all);
        assert_eq!(obs.content_chunks, 2);
        assert_eq!(
            obs.usage,
            Some(EngineUsage {
                prompt_tokens: 10,
                cached_tokens: 4,
                completion_tokens: 2
            })
        );
        assert!(obs.first_content_at.is_some());
    }

    #[test]
    fn strips_usage_chunk_when_client_did_not_ask() {
        let mut inspector = SseInspector::new(true);
        let mut obs = Observations::default();
        let out = inspector.push(&[CONTENT, USAGE, DONE].concat(), &mut obs, Instant::now());
        assert_eq!(out.to_vec(), [CONTENT, DONE].concat());
        assert!(obs.usage.is_some());
    }
}
