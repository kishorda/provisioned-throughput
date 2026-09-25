//! Against a real `tokenizer.json`. Set `PT_TEST_TOKENIZER` to a GPT-2 tokenizer (for
//! example `openai-community/gpt2`'s) to run these; skipped otherwise. Prints timings, so
//! run with `--release --nocapture` to see what a long prompt costs.

use std::time::Instant;

use pt_tokenize::{Method, TokenizerSpec, Tokenizers};

fn gpt2() -> Option<Tokenizers> {
    let Ok(path) = std::env::var("PT_TEST_TOKENIZER") else {
        eprintln!("PT_TEST_TOKENIZER not set; skipping");
        return None;
    };
    Some(
        Tokenizers::load(
            &[TokenizerSpec {
                model: "gpt2".into(),
                path: path.into(),
                message_overhead: 0,
            }],
            10_000,
        )
        .unwrap(),
    )
}

fn one(content: &str) -> Vec<(String, String)> {
    vec![(String::new(), content.to_string())]
}

#[test]
fn gpt2_counts_match_the_reference() {
    let Some(t) = gpt2() else { return };
    assert_eq!(t.method("gpt2"), Method::Tokenizer);
    // Reference counts from the Python `tokenizers` library.
    assert_eq!(t.count("gpt2", &one("Hello world")), 2);
    assert_eq!(t.count("gpt2", &one("Hello, world!")), 4);
    // Code and non-English text are far from 4 bytes per token.
    let cjk = "你好，世界。今天天气很好。";
    let n = t.count("gpt2", &one(cjk));
    assert!(
        n as usize > cjk.len() / 4 * 2,
        "{n} tokens for {} bytes",
        cjk.len()
    );
}

#[test]
fn long_prompt_cost_and_cache() {
    let Some(t) = gpt2() else { return };
    let paragraph = "The quick brown fox jumps over the lazy dog, and then runs into the forest \
                     where it finds a river. fn main() { println!(\"{}\", 42); } ";
    let long: String = paragraph.repeat(128_000 * 4 / paragraph.len());
    let msgs = one(&long);
    let start = Instant::now();
    let n = t.count("gpt2", &msgs);
    let cold = start.elapsed();
    let start = Instant::now();
    assert_eq!(t.count("gpt2", &msgs), n);
    let warm = start.elapsed();
    eprintln!(
        "{} bytes -> {n} tokens ({:.2} bytes/token): cold {cold:?}, cached {warm:?}",
        long.len(),
        long.len() as f64 / n as f64
    );
    assert!(warm < cold / 10, "the cache avoids re-tokenizing");
}

#[test]
fn cost_of_small_messages() {
    let Some(t) = gpt2() else { return };
    for kb in [1, 4, 8, 16] {
        // Distinct text each time, so nothing is cached.
        let text: String = (0..kb * 1024 / 8)
            .map(|i| format!("w{i:05} "))
            .collect::<String>();
        let start = Instant::now();
        let n = t.count("gpt2", &one(&text));
        eprintln!("{kb} KB -> {n} tokens: {:?}", start.elapsed());
    }
}
