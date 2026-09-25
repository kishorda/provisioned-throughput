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
    /// Tokens the chat template adds per message (role markers, separators).
    #[serde(default = "default_overhead")]
    pub message_overhead: u64,
}

fn default_overhead() -> u64 {
    MESSAGE_OVERHEAD
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
}

struct ModelTokenizer {
    tokenizer: tokenizers::Tokenizer,
    overhead: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct Learned {
    bytes_per_token: Option<f64>,
    engine_to_estimate: Option<f64>,
    observations: u64,
    exact_messages: u64,
    estimated_messages: u64,
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
            let entry = ModelTokenizer {
                tokenizer,
                overhead: s.message_overhead,
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
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        messages
            .iter()
            .filter(|(role, content)| cache.get(self.key(model, role, content)).is_none())
            .map(|(role, content)| role.len() + content.len())
            .sum()
    }

    /// Input tokens of a chat request, tokenizing every message however long it is.
    pub fn count(&self, model: &str, messages: &[(String, String)]) -> u64 {
        self.count_within(model, messages, usize::MAX).tokens
    }

    /// Input tokens of a chat request: every message's role and content, plus the chat
    /// template's per-message overhead. Tokenizes at most `budget` bytes of uncached
    /// messages; those that don't fit are estimated by ratio and returned as `deferred`.
    pub fn count_within(&self, model: &str, messages: &[(String, String)], budget: usize) -> Count {
        let ratio = self.bytes_per_token(model);
        let by_ratio = |role: &str, content: &str| {
            MESSAGE_OVERHEAD + ratio_tokens(role.len(), ratio) + ratio_tokens(content.len(), ratio)
        };
        let Some(t) = self.tokenizer(model) else {
            let tokens = messages.iter().map(|(r, c)| by_ratio(r, c)).sum();
            self.tally(model, 0, messages.len() as u64);
            return Count {
                tokens,
                exact: false,
                deferred: vec![],
            };
        };
        let mut left = budget;
        let mut tokens = 0;
        let mut deferred = Vec::new();
        let (mut exact_n, mut estimated_n) = (0, 0);
        for (role, content) in messages {
            let key = self.key(model, role, content);
            if let Some(n) = self.cached(key) {
                tokens += n;
                exact_n += 1;
                continue;
            }
            let bytes = role.len() + content.len();
            if bytes <= left {
                left -= bytes;
                tokens += self.tokenize(model, t, key, role, content);
                exact_n += 1;
                continue;
            }
            // Too long to tokenize now: estimate, and tokenize it in the background once.
            // Shorter messages after it may still fit what's left of the budget.
            tokens +=
                t.overhead + ratio_tokens(role.len(), ratio) + ratio_tokens(content.len(), ratio);
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
        Count {
            tokens,
            exact: estimated_n == 0,
            deferred,
        }
    }

    /// Tokenize and cache `messages` (the `deferred` part of a [`Count`]). Slow: run it on
    /// a blocking thread.
    pub fn fill(&self, model: &str, messages: &[(String, String)]) {
        let Some(t) = self.tokenizer(model) else {
            return;
        };
        for (role, content) in messages {
            let key = self.key(model, role, content);
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
        let n = t.overhead + encode(&t.tokenizer, role) + content_tokens;
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
        let c = t.count_within("m", &m, 100);
        assert!(!c.exact);
        assert_eq!(c.deferred, msgs(&[("user", &long)]));
        // Estimated at 4 bytes per token until something is learned: 4 + 1 + 300.
        assert_eq!(c.tokens, (4 + 1 + 1) + 305);
        // Asked again while it's being filled: estimated, but not deferred twice.
        assert!(t.count_within("m", &m, 100).deferred.is_empty());
        t.fill("m", &c.deferred);
        let c = t.count_within("m", &m, 100);
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
