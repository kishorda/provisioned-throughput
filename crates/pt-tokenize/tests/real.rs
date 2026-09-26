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
                ..Default::default()
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

/// Qwen2.5's tokenizer and chat template. Set `PT_TEST_TOKENIZER_DIR` to a directory with
/// `tokenizer.json` and `tokenizer_config.json` (for example Qwen/Qwen2.5-0.5B-Instruct's).
fn qwen() -> Option<(Tokenizers, pt_tokenize::ChatTemplate, tokenizers::Tokenizer)> {
    let Ok(dir) = std::env::var("PT_TEST_TOKENIZER_DIR") else {
        eprintln!("PT_TEST_TOKENIZER_DIR not set; skipping");
        return None;
    };
    let dir = std::path::PathBuf::from(dir);
    let t = Tokenizers::load(
        &[TokenizerSpec {
            model: "qwen".into(),
            path: dir.join("tokenizer.json"),
            ..Default::default()
        }],
        10_000,
    )
    .unwrap();
    let template = pt_tokenize::ChatTemplate::load(&dir.join("tokenizer_config.json")).unwrap();
    let raw = tokenizers::Tokenizer::from_file(dir.join("tokenizer.json")).unwrap();
    Some((t, template, raw))
}

fn pairs(messages: &serde_json::Value) -> Vec<(String, String)> {
    messages
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["role"].as_str().unwrap().to_string(),
                m["content"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect()
}

#[test]
fn template_counts_match_the_rendered_prompt() {
    use serde_json::json;
    let Some((t, template, raw)) = qwen() else {
        return;
    };
    assert!(t
        .status()
        .iter()
        .any(|s| s.model == "qwen" && s.chat_template));
    let tools = json!([
        { "type": "function", "function": {
            "name": "search_code",
            "description": "Search the repository for a regular expression and return matching lines with file names and line numbers.",
            "parameters": { "type": "object", "properties": {
                "pattern": { "type": "string", "description": "A regular expression." },
                "path": { "type": "string", "description": "Directory to search, relative to the repository root." },
                "max_results": { "type": "integer", "description": "Stop after this many matches." }
            }, "required": ["pattern"] }
        } },
        { "type": "function", "function": {
            "name": "read_file",
            "description": "Read a file and return its contents.",
            "parameters": { "type": "object", "properties": {
                "path": { "type": "string" }
            }, "required": ["path"] }
        } }
    ]);
    let cases = [
        (
            "chat",
            json!([{ "role": "user", "content": "Plan a three-day trip to Lisbon." }]),
            None,
        ),
        (
            "system and turns",
            json!([
                { "role": "system", "content": "You are a terse assistant. Answer in one sentence." },
                { "role": "user", "content": "What is a capacity unit?" },
                { "role": "assistant", "content": "A fixed rate of work units per second at a latency tier." },
                { "role": "user", "content": "And a work unit?" }
            ]),
            None,
        ),
        (
            "agent with tools",
            json!([
                { "role": "user", "content": "Where is the debt bucket implemented?" },
                { "role": "assistant", "content": "", "tool_calls": [
                    { "type": "function", "function": { "name": "search_code", "arguments": { "pattern": "struct DebtBucket", "max_results": 5 } } }
                ] },
                { "role": "tool", "content": "crates/pt-admission/src/bucket.rs:12: pub struct DebtBucket {" },
                { "role": "user", "content": "Read it." }
            ]),
            Some(tools.clone()),
        ),
    ];
    // The old count: 3 tokens of overhead a message, no template (a tokenizer alone).
    let bare_dir = std::env::temp_dir().join(format!("pt-bare-{}", std::process::id()));
    std::fs::create_dir_all(&bare_dir).unwrap();
    let dir = std::path::PathBuf::from(std::env::var("PT_TEST_TOKENIZER_DIR").unwrap());
    std::fs::copy(dir.join("tokenizer.json"), bare_dir.join("tokenizer.json")).unwrap();
    let bare = Tokenizers::load(
        &[TokenizerSpec {
            model: "qwen".into(),
            path: bare_dir.join("tokenizer.json"),
            ..Default::default()
        }],
        10_000,
    )
    .unwrap();
    for (name, messages, tools) in cases {
        let request = json!({ "messages": messages, "tools": tools });
        let reference = raw
            .encode_fast(template.render(&messages, tools.as_ref()).unwrap(), false)
            .unwrap()
            .len() as i64;
        let start = Instant::now();
        let c = t.count_within("qwen", &pairs(&messages), Some(&request), usize::MAX);
        let cold = start.elapsed();
        let start = Instant::now();
        t.count_within("qwen", &pairs(&messages), Some(&request), usize::MAX);
        let warm = start.elapsed();
        let old = bare.count("qwen", &pairs(&messages));
        let before = t.count_within("qwen", &pairs(&messages), None, usize::MAX);
        let err = c.tokens as i64 - reference;
        eprintln!(
            "{name}: reference {reference}, counted {} (framing {}), off by {err}; without tools {}, old overhead count {old}; cold {cold:?}, cached {warm:?}",
            c.tokens, c.framing, before.tokens
        );
        // Tokens can merge across a text/template boundary: allow one per message.
        assert!(
            err.unsigned_abs() as usize <= messages.as_array().unwrap().len(),
            "{name}: {} vs {reference}",
            c.tokens
        );
    }
}
