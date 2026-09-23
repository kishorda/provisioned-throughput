//! Token counting.
//!
//! The gateway needs input token counts before admission. Production uses the model's own
//! tokenizer (HF `tokenizers`). Until that is wired in, [`ApproxTokenCounter`] gives a
//! deterministic estimate. Settlement uses the engine's reported counts, so estimation
//! error only affects the admission estimate, not billing.

pub trait TokenCounter: Send + Sync {
    fn count(&self, text: &str) -> u64;
}

/// Tokens added per chat message for role markers and separators.
pub const MESSAGE_OVERHEAD: u64 = 3;

/// Tokens for one chat message, including framing.
pub fn count_message(counter: &dyn TokenCounter, role: &str, content: &str) -> u64 {
    MESSAGE_OVERHEAD + counter.count(role) + counter.count(content)
}

/// Roughly 4 bytes per token, which is typical for English text on BPE tokenizers.
#[derive(Debug, Clone, Copy, Default)]
pub struct ApproxTokenCounter;

impl TokenCounter for ApproxTokenCounter {
    fn count(&self, text: &str) -> u64 {
        (text.len() as u64).div_ceil(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approx_counts() {
        let t = ApproxTokenCounter;
        assert_eq!(t.count(""), 0);
        assert_eq!(t.count("abcd"), 1);
        assert_eq!(t.count("abcde"), 2);
        // 3 framing + 1 ("user") + 2 ("hello")
        assert_eq!(count_message(&t, "user", "hello"), 6);
    }
}
