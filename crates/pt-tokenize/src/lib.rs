//! Input token counting for admission (docs/04 §3, ADR-028).
//!
//! The gateway counts a request's input tokens before it admits it, so the prefill part of
//! the WU estimate is as exact as it can afford. Each model is counted one of two ways:
//!
//! - **Tokenizer.** The model's own `tokenizer.json` (Hugging Face `tokenizers`). A chat
//!   message costs its content and role tokens plus a per-model `message_overhead` for the
//!   chat template's markers. Counts are cached per message, so an agent that resends its
//!   whole conversation every turn only pays for the new messages.
//! - **Learned ratio.** Models without a tokenizer file are counted at a bytes-per-token
//!   ratio, starting at 4 and learned from the prompt counts the engine reports.
//!
//! **Budget.** Tokenizing is slow: a new 512 KB message (about 128K tokens) takes hundreds
//! of milliseconds. [`Tokenizers::count_within`] tokenizes at most a byte budget of
//! uncached messages before admission. Longer new messages are estimated at the model's
//! bytes-per-token ratio, which for tokenizer models is learned from its own exact counts,
//! and returned as `deferred`. The caller tokenizes those in the background
//! ([`Tokenizers::fill`]), so the next request that repeats them is exact.
//!
//! [`Tokenizers::observe`] compares each estimate with the engine's count, so the gateway
//! can report how accurate each model's counting is.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, RandomState};
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod template;
pub use template::{ChatTemplate, Framing, TemplateError};

pub use pt_core::tokens::MESSAGE_OVERHEAD;

/// Model name that matches any model without its own entry.
pub const ANY_MODEL: &str = "*";

/// Bytes per token before anything is learned: typical for English on BPE tokenizers.
pub const DEFAULT_BYTES_PER_TOKEN: f64 = 4.0;

/// Messages longer than this are split at whitespace and encoded in parallel.
const CHUNK_BYTES: usize = 16 * 1024;

/// Weight of each new observation in the moving averages.
const ALPHA: f64 = 0.05;

/// One model's tokenizer.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenizerSpec {
    /// The model name, as in reservations (or `*` for any model).
    pub model: String,
    /// Path to the model's `tokenizer.json`.
    pub path: PathBuf,
    /// The model's chat template: a `tokenizer_config.json` or a `.jinja` file (ADR-032).
    /// Defaults to `tokenizer_config.json` beside `path`, if it has a `chat_template`.
    #[serde(default)]
    pub chat_template: Option<PathBuf>,
    /// Without a chat template: tokens it adds per message (role markers, separators).
    #[serde(default = "default_overhead")]
    pub message_overhead: u64,
    /// Tokens one image part costs. The template only adds a marker for it. Placeholder:
    /// depends on the model's vision encoder and the image size.
    #[serde(default = "default_image_tokens")]
    pub image_tokens: u64,
}

impl Default for TokenizerSpec {
    fn default() -> Self {
        Self {
            model: String::new(),
            path: PathBuf::new(),
            chat_template: None,
            message_overhead: default_overhead(),
            image_tokens: default_image_tokens(),
        }
    }
}

fn default_overhead() -> u64 {
    MESSAGE_OVERHEAD
}

/// Placeholder until calibrated per model.
pub const DEFAULT_IMAGE_TOKENS: u64 = 576;

fn default_image_tokens() -> u64 {
    DEFAULT_IMAGE_TOKENS
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("loading the tokenizer for {model} from {path}: {message}")]
    Tokenizer {
        model: String,
        path: String,
        message: String,
    },
    #[error("two tokenizers for model {0}")]
    Duplicate(String),
    #[error("chat template for {model}: {source}")]
    Template {
        model: String,
        source: TemplateError,
    },
}

/// How a model's tokens are counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    Tokenizer,
    Ratio,
}

/// A request's input token count.
#[derive(Debug, Clone, PartialEq)]
pub struct Count {
    pub tokens: u64,
    /// Each message's tokens, in order. They sum to `tokens`. The chat template's
    /// framing (tools, system defaults, generation prompt) is counted in the first.
    pub per_message: Vec<u64>,
    /// Tokens the chat template adds around message text, and image tokens. Included in
    /// `per_message`.
    pub framing: u64,
    /// Every message was counted by the tokenizer (now or from the cache).
    pub exact: bool,
    /// Messages estimated by ratio because they didn't fit the budget. Tokenize them in
    /// the background with [`Tokenizers::fill`].
    pub deferred: Vec<(String, String)>,
}

/// What the gateway reports about one model's counting.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModelStatus {
    pub model: String,
    pub method: Method,
    /// The ratio used for messages that aren't tokenized: learned from the engine for
    /// `ratio` models, and from the tokenizer's own counts for `tokenizer` models.
    pub bytes_per_token: f64,
    /// Moving average of engine prompt tokens ÷ the gateway's estimate. 1.0 is exact.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine_to_estimate: Option<f64>,
    pub observations: u64,
    /// Messages counted exactly (tokenized now or cached) and by ratio.
    pub exact_messages: u64,
    pub estimated_messages: u64,
    /// The model's chat template is rendered to count framing (ADR-032). Otherwise each
    /// message adds `message_overhead`.
    pub chat_template: bool,
    /// Requests the template couldn't render, counted with `message_overhead` instead.
    pub template_errors: u64,
}

struct ModelTokenizer {
    tokenizer: tokenizers::Tokenizer,
    overhead: u64,
    /// With a template, messages are counted by their text alone, and the template's
    /// framing separately.
    template: Option<ChatTemplate>,
    image_tokens: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct Learned {
    bytes_per_token: Option<f64>,
    engine_to_estimate: Option<f64>,
    observations: u64,
    exact_messages: u64,
    estimated_messages: u64,
    template_errors: u64,
}

/// A bounded map from message hash to token count. Oldest entries go first.
struct Cache {
    counts: HashMap<u64, u64>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl Cache {
    fn get(&self, key: u64) -> Option<u64> {
        self.counts.get(&key).copied()
    }

    fn insert(&mut self, key: u64, tokens: u64) {
        if self.capacity == 0 {
            return;
        }
        if self.counts.insert(key, tokens).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.counts.remove(&old);
            }
        }
    }
}

/// Every model's token counter, shared by all requests.
pub struct Tokenizers {
    models: HashMap<String, ModelTokenizer>,
    learned: Mutex<HashMap<String, Learned>>,
    cache: Mutex<Cache>,
    /// Messages being tokenized in the background, so each is filled once.
    filling: Mutex<HashSet<u64>>,
    /// Keys are random per process, so clients can't aim collisions at the cache.
    hasher: RandomState,
}

impl std::fmt::Debug for Tokenizers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut models: Vec<_> = self.models.keys().collect();
        models.sort();
        f.debug_struct("Tokenizers")
            .field("models", &models)
            .finish()
    }
}

impl Tokenizers {
    /// No tokenizer files: every model is counted by its learned ratio.
    pub fn ratio_only() -> Self {
        Self::with_models(HashMap::new(), 0)
    }

    /// Load each spec's `tokenizer.json`. `cache_entries` bounds the per-message cache.
    pub fn load(specs: &[TokenizerSpec], cache_entries: usize) -> Result<Self, LoadError> {
        let mut models = HashMap::new();
        for s in specs {
            let tokenizer =
                tokenizers::Tokenizer::from_file(&s.path).map_err(|e| LoadError::Tokenizer {
                    model: s.model.clone(),
                    path: s.path.display().to_string(),
                    message: e.to_string(),
                })?;
            // The first encode initialises lazily built state; don't make a request pay it.
            let _ = tokenizer.encode_fast("Hello, world.", false);
            let template = match &s.chat_template {
                Some(path) => Some(ChatTemplate::load(path).map_err(|e| LoadError::Template {
                    model: s.model.clone(),
                    source: e,
                })?),
                // Beside the tokenizer, as Hugging Face ships it. Absent or without a
                // chat_template: count by message_overhead.
                None => s
                    .path
                    .parent()
                    .map(|d| d.join("tokenizer_config.json"))
                    .filter(|p| p.exists())
                    .and_then(|p| match ChatTemplate::load(&p) {
                        Ok(t) => Some(Ok(t)),
                        Err(TemplateError::Missing { .. }) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .transpose()
                    .map_err(|e| LoadError::Template {
                        model: s.model.clone(),
                        source: e,
                    })?,
            };
            let entry = ModelTokenizer {
                tokenizer,
                overhead: s.message_overhead,
                template,
                image_tokens: s.image_tokens,
            };
            if models.insert(s.model.clone(), entry).is_some() {
                return Err(LoadError::Duplicate(s.model.clone()));
            }
        }
        Ok(Self::with_models(models, cache_entries))
    }

    fn with_models(models: HashMap<String, ModelTokenizer>, cache_entries: usize) -> Self {
        Self {
            models,
            learned: Mutex::new(HashMap::new()),
            cache: Mutex::new(Cache {
                counts: HashMap::new(),
                order: VecDeque::new(),
                capacity: cache_entries,
            }),
            filling: Mutex::new(HashSet::new()),
            hasher: RandomState::new(),
        }
    }

    fn tokenizer(&self, model: &str) -> Option<&ModelTokenizer> {
        self.models
            .get(model)
            .or_else(|| self.models.get(ANY_MODEL))
    }

    pub fn method(&self, model: &str) -> Method {
        if self.tokenizer(model).is_some() {
            Method::Tokenizer
        } else {
            Method::Ratio
        }
    }

    fn key(&self, model: &str, role: &str, content: &str) -> u64 {
        self.hasher.hash_one((model, role, content))
    }

    /// A message's cache key. With a template a message is counted by its text alone, so
    /// the role isn't part of it, and the key can't collide with the overhead scheme's.
    fn message_key(&self, model: &str, t: &ModelTokenizer, role: &str, content: &str) -> u64 {
        if t.template.is_some() {
            self.hasher.hash_one(("text", model, content))
        } else {
            self.key(model, role, content)
        }
    }

    fn cached(&self, key: u64) -> Option<u64> {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
    }

    /// Bytes that [`Tokenizers::count`] would have to tokenize: messages not in the cache.
    /// Zero for ratio models, which are cheap to count.
    pub fn uncached_bytes(&self, model: &str, messages: &[(String, String)]) -> usize {
        if self.tokenizer(model).is_none() {
            return 0;
        }
        let Some(t) = self.tokenizer(model) else {
            return 0;
        };
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        messages
            .iter()
            .filter(|(role, content)| {
                cache
                    .get(self.message_key(model, t, role, content))
                    .is_none()
            })
            .map(|(role, content)| role.len() + content.len())
            .sum()
    }

    /// Input tokens of a chat request, tokenizing every message however long it is.
    pub fn count(&self, model: &str, messages: &[(String, String)]) -> u64 {
        self.count_within(model, messages, None, usize::MAX).tokens
    }

    /// Input tokens of a chat request. `messages` are the (role, text) pairs, and
    /// `request` is the request JSON when there is one: its `messages` (with tool calls and
    /// image parts) and `tools` are rendered through the model's chat template (ADR-032).
    /// Message text is tokenized and cached per message, at most `budget` bytes of it now;
    /// messages that don't fit are estimated by ratio and returned as `deferred`.
    pub fn count_within(
        &self,
        model: &str,
        messages: &[(String, String)],
        request: Option<&Value>,
        budget: usize,
    ) -> Count {
        let ratio = self.bytes_per_token(model);
        let by_ratio = |role: &str, content: &str| {
            MESSAGE_OVERHEAD + ratio_tokens(role.len(), ratio) + ratio_tokens(content.len(), ratio)
        };
        let Some(t) = self.tokenizer(model) else {
            let per_message: Vec<u64> = messages.iter().map(|(r, c)| by_ratio(r, c)).collect();
            self.tally(model, 0, messages.len() as u64);
            return Count {
                tokens: per_message.iter().sum(),
                per_message,
                framing: 0,
                exact: false,
                deferred: vec![],
            };
        };
        let mut left = budget;
        let mut per_message = Vec::with_capacity(messages.len());
        let mut deferred = Vec::new();
        let (mut exact_n, mut estimated_n) = (0, 0);
        for (role, content) in messages {
            let key = self.message_key(model, t, role, content);
            if let Some(n) = self.cached(key) {
                per_message.push(n);
                exact_n += 1;
                continue;
            }
            let bytes = role.len() + content.len();
            if bytes <= left {
                left -= bytes;
                per_message.push(self.tokenize(model, t, key, role, content));
                exact_n += 1;
                continue;
            }
            // Too long to tokenize now: estimate, and tokenize it in the background once.
            // Shorter messages after it may still fit what's left of the budget.
            per_message.push(match t.template {
                Some(_) => ratio_tokens(content.len(), ratio),
                None => {
                    t.overhead
                        + ratio_tokens(role.len(), ratio)
                        + ratio_tokens(content.len(), ratio)
                }
            });
            estimated_n += 1;
            if self
                .filling
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key)
            {
                deferred.push((role.clone(), content.clone()));
            }
        }
        self.tally(model, exact_n, estimated_n);
        let framing = self.add_framing(model, t, messages, request, &mut per_message);
        Count {
            tokens: per_message.iter().sum(),
            per_message,
            framing,
            exact: estimated_n == 0,
            deferred,
        }
    }

    /// Add what the chat template renders around message text, and image tokens, to
    /// `per_message`. Returns how much was added.
    fn add_framing(
        &self,
        model: &str,
        t: &ModelTokenizer,
        messages: &[(String, String)],
        request: Option<&Value>,
        per_message: &mut [u64],
    ) -> u64 {
        let built;
        let json = match request.and_then(|r| r.get("messages")) {
            Some(m) => m,
            None => {
                built = Value::Array(
                    messages
                        .iter()
                        .map(|(r, c)| serde_json::json!({ "role": r, "content": c }))
                        .collect(),
                );
                &built
            }
        };
        let mut added = 0;
        // Images, in the message that carries them.
        for (i, m) in json.as_array().into_iter().flatten().enumerate() {
            let images = m
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|p| {
                    matches!(
                        p.get("type").and_then(Value::as_str),
                        Some("image_url" | "image" | "input_image")
                    )
                })
                .count() as u64;
            if let Some(n) = per_message.get_mut(i) {
                *n += images * t.image_tokens;
                added += images * t.image_tokens;
            }
        }
        let Some(template) = &t.template else {
            return added;
        };
        let tools = request.and_then(|r| r.get("tools"));
        match template.framing(json, tools) {
            Ok(f) => {
                let key = self.hasher.hash_one(("framing", model, f.text.as_str()));
                let n = match self.cached(key) {
                    Some(n) => n,
                    None => {
                        let n = encode(&t.tokenizer, &f.text);
                        self.cache
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(key, n);
                        n
                    }
                };
                // Mostly at the start: tools and system defaults come first.
                if let Some(first) = per_message.first_mut() {
                    *first += n;
                }
                added + n
            }
            Err(e) => {
                // Count as if there were no template, and say so in the status.
                tracing::debug!(%model, error = %e, "chat template failed");
                let mut learned = self.learned.lock().unwrap_or_else(|e| e.into_inner());
                learned
                    .entry(model.to_string())
                    .or_default()
                    .template_errors += 1;
                drop(learned);
                for ((role, _), n) in messages.iter().zip(per_message.iter_mut()) {
                    let extra = t.overhead + encode(&t.tokenizer, role);
                    *n += extra;
                    added += extra;
                }
                added
            }
        }
    }

    /// Tokenize and cache `messages` (the `deferred` part of a [`Count`]). Slow: run it on
    /// a blocking thread.
    pub fn fill(&self, model: &str, messages: &[(String, String)]) {
        let Some(t) = self.tokenizer(model) else {
            return;
        };
        for (role, content) in messages {
            let key = self.message_key(model, t, role, content);
            if self.cached(key).is_none() {
                self.tokenize(model, t, key, role, content);
            }
            self.filling
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
        }
    }

    /// Tokenize one message, cache it, and learn the model's bytes-per-token from it.
    fn tokenize(
        &self,
        model: &str,
        t: &ModelTokenizer,
        key: u64,
        role: &str,
        content: &str,
    ) -> u64 {
        let content_tokens = encode(&t.tokenizer, content);
        // With a template, roles and markers are counted in its framing.
        let n = match t.template {
            Some(_) => content_tokens,
            None => t.overhead + encode(&t.tokenizer, role) + content_tokens,
        };
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, n);
        // Short messages say little about the ratio of long ones.
        if content.len() >= 256 && content_tokens > 0 {
            let r = content.len() as f64 / content_tokens as f64;
            let mut learned = self.learned.lock().unwrap_or_else(|e| e.into_inner());
            let l = learned.entry(model.to_string()).or_default();
            l.bytes_per_token = Some(ewma(l.bytes_per_token, r));
        }
        n
    }

    fn tally(&self, model: &str, exact: u64, estimated: u64) {
        let mut learned = self.learned.lock().unwrap_or_else(|e| e.into_inner());
        let l = learned.entry(model.to_string()).or_default();
        l.exact_messages += exact;
        l.estimated_messages += estimated;
    }

    /// The bytes-per-token ratio for messages that aren't tokenized.
    pub fn bytes_per_token(&self, model: &str) -> f64 {
        self.learned
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(model)
            .and_then(|l| l.bytes_per_token)
            .unwrap_or(DEFAULT_BYTES_PER_TOKEN)
    }

    /// Learn from a settled request: its message count, the prompt's `bytes` (roles and
    /// contents), the gateway's `estimate`, and the prompt tokens the engine reported.
    pub fn observe(&self, model: &str, messages: usize, bytes: usize, estimate: u64, engine: u64) {
        if engine == 0 || estimate == 0 {
            return;
        }
        let ratio_model = self.tokenizer(model).is_none();
        let mut learned = self.learned.lock().unwrap_or_else(|e| e.into_inner());
        let l = learned.entry(model.to_string()).or_default();
        l.observations += 1;
        let seen = engine as f64 / estimate as f64;
        l.engine_to_estimate = Some(ewma(l.engine_to_estimate, seen));
        if ratio_model {
            // Framing tokens don't scale with bytes; leave them out of the ratio.
            let content = engine.saturating_sub(MESSAGE_OVERHEAD * messages as u64);
            if content > 0 && bytes > 0 {
                let r = (bytes as f64 / content as f64).clamp(1.0, 16.0);
                l.bytes_per_token = Some(ewma(l.bytes_per_token, r));
            }
        }
    }

    /// Every model the gateway has a tokenizer for or has counted.
    pub fn status(&self) -> Vec<ModelStatus> {
        let learned = self.learned.lock().unwrap_or_else(|e| e.into_inner());
        let mut models: Vec<&String> = self.models.keys().chain(learned.keys()).collect();
        models.sort();
        models.dedup();
        models
            .into_iter()
            .map(|m| {
                let l = learned.get(m).copied().unwrap_or_default();
                ModelStatus {
                    model: m.clone(),
                    method: self.method(m),
                    bytes_per_token: l.bytes_per_token.unwrap_or(DEFAULT_BYTES_PER_TOKEN),
                    engine_to_estimate: l.engine_to_estimate,
                    observations: l.observations,
                    exact_messages: l.exact_messages,
                    estimated_messages: l.estimated_messages,
                    chat_template: self.tokenizer(m).is_some_and(|t| t.template.is_some()),
                    template_errors: l.template_errors,
                }
            })
            .collect()
    }
}

fn ewma(prev: Option<f64>, x: f64) -> f64 {
    match prev {
        Some(p) => p + ALPHA * (x - p),
        None => x,
    }
}

fn ratio_tokens(bytes: usize, ratio: f64) -> u64 {
    (bytes as f64 / ratio).ceil() as u64
}

/// Token count of `text`, without special tokens (the template overhead covers those).
/// Long text is split just before a space that follows a non-space, where BPE and
/// SentencePiece tokenizers start a new word anyway, and the chunks are encoded in
/// parallel.
fn encode(t: &tokenizers::Tokenizer, text: &str) -> u64 {
    if text.is_empty() {
        return 0;
    }
    let fallback = || ratio_tokens(text.len(), DEFAULT_BYTES_PER_TOKEN);
    if text.len() <= CHUNK_BYTES {
        return t
            .encode_fast(text, false)
            .map_or_else(|_| fallback(), |e| e.len() as u64);
    }
    let chunks = split_at_words(text, CHUNK_BYTES);
    match t.encode_batch_fast(chunks, false) {
        Ok(es) => es.iter().map(|e| e.len() as u64).sum(),
        // Never fails for plain text in practice; fall back rather than reject.
        Err(_) => fallback(),
    }
}

fn split_at_words(text: &str, size: usize) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let mut end = (start + size).min(bytes.len());
        while end < bytes.len() && !(bytes[end] == b' ' && !bytes[end - 1].is_ascii_whitespace()) {
            end += 1;
        }
        // `end` is at a space or the end, both char boundaries.
        out.push(&text[start..end]);
        start = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Tokenizers {
        Tokenizers::load(
            &[TokenizerSpec {
                model: "m".into(),
                path: concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/wordlevel.json").into(),
                message_overhead: 4,
                ..Default::default()
            }],
            100,
        )
        .unwrap()
    }

    fn msgs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(r, c)| (r.to_string(), c.to_string()))
            .collect()
    }

    #[test]
    fn counts_with_the_model_tokenizer() {
        let t = fixture();
        assert_eq!(t.method("m"), Method::Tokenizer);
        // "hello, world." is 4 word-level tokens; "user" 1; overhead 4.
        assert_eq!(t.count("m", &msgs(&[("user", "hello, world.")])), 9);
        // Unknown words are still one token each.
        assert_eq!(t.count("m", &msgs(&[("user", "zebra quagga")])), 7);
    }

    #[test]
    fn repeated_messages_come_from_the_cache() {
        let t = fixture();
        let turn1 = msgs(&[("system", "hello world"), ("user", "hello")]);
        assert_eq!(t.uncached_bytes("m", &turn1), 6 + 11 + 4 + 5);
        let first = t.count("m", &turn1);
        assert_eq!(t.uncached_bytes("m", &turn1), 0);
        // The next turn resends the conversation: only the new message is uncached.
        let mut turn2 = turn1.clone();
        turn2.push(("assistant".into(), "world".into()));
        assert_eq!(t.uncached_bytes("m", &turn2), 9 + 5);
        assert_eq!(t.count("m", &turn2), first + 4 + 1 + 1);
        // The cache is keyed by model too.
        assert_eq!(
            t.uncached_bytes("other", &turn1),
            0,
            "ratio models aren't cached"
        );
    }

    #[test]
    fn cache_is_bounded() {
        let t = Tokenizers::load(
            &[TokenizerSpec {
                model: "m".into(),
                path: concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/wordlevel.json").into(),
                message_overhead: 4,
                ..Default::default()
            }],
            2,
        )
        .unwrap();
        for w in ["a", "b", "c"] {
            t.count("m", &msgs(&[("user", w)]));
        }
        assert!(
            t.uncached_bytes("m", &msgs(&[("user", "a")])) > 0,
            "evicted"
        );
        assert_eq!(t.uncached_bytes("m", &msgs(&[("user", "c")])), 0);
    }

    #[test]
    fn unknown_models_learn_a_ratio_from_the_engine() {
        let t = fixture();
        let m = msgs(&[("user", &"x".repeat(400))]);
        // 3 framing + ceil(4 / 4) + 400 / 4.
        assert_eq!(t.count("code-model", &m), 104);
        // The engine keeps counting 3 bytes per token: 3 + 2 + 134 ≈ 139 tokens.
        for _ in 0..200 {
            t.observe("code-model", 1, 404, t.count("code-model", &m), 139);
        }
        let r = t.bytes_per_token("code-model");
        assert!((r - 404.0 / 136.0).abs() < 0.01, "learned {r}");
        let now = t.count("code-model", &m);
        assert!((138..=141).contains(&now), "{now}");
        let s = t.status();
        let code = s.iter().find(|s| s.model == "code-model").unwrap();
        assert_eq!(code.method, Method::Ratio);
        assert!((code.engine_to_estimate.unwrap() - 1.0).abs() < 0.05);
        assert_eq!(
            s.iter().find(|s| s.model == "m").unwrap().method,
            Method::Tokenizer
        );
    }

    #[test]
    fn wildcard_and_load_errors() {
        let t = Tokenizers::load(
            &[TokenizerSpec {
                model: ANY_MODEL.into(),
                path: concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/wordlevel.json").into(),
                message_overhead: 0,
                ..Default::default()
            }],
            10,
        )
        .unwrap();
        assert_eq!(t.method("anything"), Method::Tokenizer);
        assert_eq!(t.count("anything", &msgs(&[("user", "hello")])), 2);
        let err = Tokenizers::load(
            &[TokenizerSpec {
                model: "m".into(),
                path: "/nonexistent/tokenizer.json".into(),
                message_overhead: 3,
                ..Default::default()
            }],
            10,
        )
        .unwrap_err();
        assert!(err.to_string().contains("tokenizer for m"), "{err}");
    }

    #[test]
    fn long_new_messages_are_estimated_then_filled() {
        let t = fixture();
        let long = "hello world ".repeat(100); // 1,200 bytes, 200 tokens
        let m = msgs(&[("system", "hello"), ("user", &long)]);
        // A 100-byte budget covers the system message but not the long one.
        let c = t.count_within("m", &m, None, 100);
        assert!(!c.exact);
        assert_eq!(c.deferred, msgs(&[("user", &long)]));
        // Estimated at 4 bytes per token until something is learned: 4 + 1 + 300.
        assert_eq!(c.tokens, (4 + 1 + 1) + 305);
        // Asked again while it's being filled: estimated, but not deferred twice.
        assert!(t.count_within("m", &m, None, 100).deferred.is_empty());
        t.fill("m", &c.deferred);
        let c = t.count_within("m", &m, None, 100);
        assert!(c.exact);
        assert_eq!(c.tokens, 6 + (4 + 1 + 200));
        // The fill taught the model's ratio: 6 bytes per token here.
        assert!((t.bytes_per_token("m") - 6.0).abs() < 1e-9);
        let s = t.status();
        let m = s.iter().find(|s| s.model == "m").unwrap();
        assert_eq!((m.exact_messages, m.estimated_messages), (1 + 1 + 2, 1 + 1));
    }

    #[test]
    fn long_text_is_chunked_at_word_starts() {
        let t = fixture();
        let text = "hello, world. ".repeat(3_000); // 42 KB, 4 tokens per repeat
        assert!(text.len() > CHUNK_BYTES * 2);
        let chunks = split_at_words(&text, CHUNK_BYTES);
        assert!(chunks.len() >= 3);
        assert_eq!(chunks.concat(), text);
        assert!(chunks[1..].iter().all(|c| c.starts_with(' ')));
        assert_eq!(encode(&t.models["m"].tokenizer, &text), 12_000);
    }
}
