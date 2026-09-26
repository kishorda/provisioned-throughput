//! Chat templates: what a model's template adds around message text (ADR-032).
//!
//! Engines turn a chat request into a prompt with the model's Jinja chat template (from
//! its `tokenizer_config.json`), then tokenize it. Besides role markers, the template can
//! add a default system prompt, every tool's JSON schema, the arguments of earlier tool
//! calls, and the generation prompt. None of that is message text.
//!
//! [`ChatTemplate::framing`] renders the conversation with each message's text replaced by
//! a placeholder, then removes the placeholders. What's left is everything the template
//! adds, which [`crate::Tokenizers`] tokenizes and caches separately from message text. So
//! message text keeps its per-message cache, and a long tool list is tokenized once.
//!
//! Rendering follows Hugging Face `apply_chat_template`: `messages`, `tools`,
//! `add_generation_prompt = true`, `bos_token`, `eos_token`, `raise_exception`,
//! `strftime_now`, Python string methods, and HF's `tojson` (`", "` and `": "`
//! separators, no HTML escaping, optional `indent`).

use std::path::Path;

use minijinja::value::{Kwargs, Value as JValue};
use minijinja::{Environment, Error, ErrorKind};
use serde_json::Value;

/// Marks where message text was. Private-use code points, so real text never contains
/// them by accident.
const OPEN: char = '\u{E000}';
const CLOSE: char = '\u{E001}';

pub struct ChatTemplate {
    env: Environment<'static>,
    bos: String,
    eos: String,
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateError {
    #[error("reading {path}: {message}")]
    Read { path: String, message: String },
    #[error("{path} has no chat_template")]
    Missing { path: String },
    #[error("chat template: {0}")]
    Render(String),
}

/// What the template adds to one request.
#[derive(Debug, Clone, PartialEq)]
pub struct Framing {
    /// The rendered prompt without message text.
    pub text: String,
    /// Image parts in the messages. The template only adds a marker for each.
    pub images: u64,
}

impl ChatTemplate {
    /// Load `chat_template`, `bos_token`, and `eos_token` from a `tokenizer_config.json`, or
    /// a bare template from any other file (a `.jinja`).
    pub fn load(path: &Path) -> Result<Self, TemplateError> {
        let read = |e: String| TemplateError::Read {
            path: path.display().to_string(),
            message: e,
        };
        let text = std::fs::read_to_string(path).map_err(|e| read(e.to_string()))?;
        if path.extension().is_some_and(|e| e == "json") {
            let config: Value = serde_json::from_str(&text).map_err(|e| read(e.to_string()))?;
            let template = match config.get("chat_template") {
                Some(Value::String(t)) => t.clone(),
                // Several named templates: use the default one.
                Some(Value::Array(named)) => named
                    .iter()
                    .find(|t| t["name"] == "default")
                    .or_else(|| named.first())
                    .and_then(|t| t["template"].as_str())
                    .map(str::to_string)
                    .ok_or_else(|| TemplateError::Missing {
                        path: path.display().to_string(),
                    })?,
                _ => {
                    return Err(TemplateError::Missing {
                        path: path.display().to_string(),
                    })
                }
            };
            let token = |key: &str| match config.get(key) {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Object(o)) => o
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                _ => String::new(),
            };
            Self::new(template, token("bos_token"), token("eos_token"))
        } else {
            Self::new(text, String::new(), String::new())
        }
    }

    pub fn new(template: String, bos: String, eos: String) -> Result<Self, TemplateError> {
        let mut env = Environment::new();
        env.set_unknown_method_callback(minijinja_contrib::pycompat::unknown_method_callback);
        env.add_filter("tojson", tojson);
        env.add_function("raise_exception", |msg: String| -> Result<String, Error> {
            Err(Error::new(ErrorKind::InvalidOperation, msg))
        });
        env.add_function("strftime_now", |fmt: String| {
            // Only affects a date line in some system prompts: its length is what counts.
            jiff::Zoned::now().strftime(&fmt).to_string()
        });
        env.add_template_owned("chat", template)
            .map_err(|e| TemplateError::Render(e.to_string()))?;
        Ok(Self { env, bos, eos })
    }

    /// Render `messages` (the request's JSON array) and `tools` with each message's text
    /// replaced by a placeholder, and return what's left.
    pub fn framing(
        &self,
        messages: &Value,
        tools: Option<&Value>,
    ) -> Result<Framing, TemplateError> {
        let mut images = 0;
        let mut marked = messages.clone();
        let mut n = 0;
        let mut mark = |s: &mut String| {
            *s = format!("{OPEN}{n}{CLOSE}");
            n += 1;
        };
        for m in marked.as_array_mut().into_iter().flatten() {
            match m.get_mut("content") {
                Some(Value::String(s)) => mark(s),
                Some(Value::Array(parts)) => {
                    for p in parts {
                        if let Some("image_url" | "image" | "input_image") =
                            p.get("type").and_then(Value::as_str)
                        {
                            images += 1;
                        }
                        if let Some(Value::String(t)) = p.get_mut("text") {
                            mark(t);
                        }
                    }
                }
                _ => {}
            }
        }
        let tools = tools.filter(|t| t.as_array().is_some_and(|a| !a.is_empty()));
        let ctx = minijinja::context! {
            messages => JValue::from_serialize(&marked),
            tools => tools.map(JValue::from_serialize),
            add_generation_prompt => true,
            bos_token => self.bos.clone(),
            eos_token => self.eos.clone(),
        };
        let rendered = self
            .env
            .get_template("chat")
            .and_then(|t| t.render(ctx))
            .map_err(|e| TemplateError::Render(e.to_string()))?;
        Ok(Framing {
            text: strip_marks(&rendered),
            images,
        })
    }

    /// The full prompt, as the engine renders it. For tests and calibration.
    pub fn render(&self, messages: &Value, tools: Option<&Value>) -> Result<String, TemplateError> {
        let tools = tools.filter(|t| t.as_array().is_some_and(|a| !a.is_empty()));
        let ctx = minijinja::context! {
            messages => JValue::from_serialize(messages),
            tools => tools.map(JValue::from_serialize),
            add_generation_prompt => true,
            bos_token => self.bos.clone(),
            eos_token => self.eos.clone(),
        };
        self.env
            .get_template("chat")
            .and_then(|t| t.render(ctx))
            .map_err(|e| TemplateError::Render(e.to_string()))
    }
}

fn strip_marks(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut inside = false;
    for c in s.chars() {
        match c {
            OPEN => inside = true,
            CLOSE => inside = false,
            _ if !inside => out.push(c),
            _ => {}
        }
    }
    out
}

/// Hugging Face's `tojson`: `json.dumps(x, ensure_ascii=False, indent=indent)`.
fn tojson(value: JValue, kwargs: Kwargs) -> Result<JValue, Error> {
    let indent: Option<usize> = kwargs.get("indent")?;
    let _: Option<JValue> = kwargs.get("separators")?;
    let _: Option<bool> = kwargs.get("sort_keys")?;
    kwargs.assert_all_used()?;
    let json: Value = serde_json::to_value(&value)
        .map_err(|e| Error::new(ErrorKind::InvalidOperation, e.to_string()))?;
    let mut out = String::new();
    write_json(&json, indent, 0, &mut out);
    Ok(JValue::from_safe_string(out))
}

fn write_json(v: &Value, indent: Option<usize>, depth: usize, out: &mut String) {
    let newline = |out: &mut String, depth: usize| {
        if let Some(i) = indent {
            out.push('\n');
            out.push_str(&" ".repeat(i * depth));
        }
    };
    let item_sep = if indent.is_some() { "," } else { ", " };
    match v {
        Value::Array(a) if !a.is_empty() => {
            out.push('[');
            for (k, x) in a.iter().enumerate() {
                if k > 0 {
                    out.push_str(item_sep);
                }
                newline(out, depth + 1);
                write_json(x, indent, depth + 1, out);
            }
            newline(out, depth);
            out.push(']');
        }
        Value::Object(o) if !o.is_empty() => {
            out.push('{');
            for (k, (key, x)) in o.iter().enumerate() {
                if k > 0 {
                    out.push_str(item_sep);
                }
                newline(out, depth + 1);
                out.push_str(&Value::String(key.clone()).to_string());
                out.push_str(": ");
                write_json(x, indent, depth + 1, out);
            }
            newline(out, depth);
            out.push('}');
        }
        other => out.push_str(&other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// ChatML with tools, in the style of Qwen's template.
    const CHATML: &str = r#"
{%- if tools %}{{- '<|im_start|>system\n# Tools' }}{%- for t in tools %}{{- '\n' + (t | tojson) }}{%- endfor %}{{- '<|im_end|>\n' }}{%- endif %}
{%- for m in messages %}{{- '<|im_start|>' + m.role + '\n' + (m.content | trim) }}
{%- if m.tool_calls %}{%- for c in m.tool_calls %}{{- '\n<tool_call>' + (c.function | tojson) + '</tool_call>' }}{%- endfor %}{%- endif %}
{{- '<|im_end|>\n' }}{%- endfor %}
{%- if add_generation_prompt %}{{- '<|im_start|>assistant\n' }}{%- endif %}"#;

    fn chatml() -> ChatTemplate {
        ChatTemplate::new(CHATML.into(), String::new(), "<|im_end|>".into()).unwrap()
    }

    #[test]
    fn framing_is_the_prompt_without_message_text() {
        let t = chatml();
        let messages = json!([
            { "role": "system", "content": "Be brief." },
            { "role": "user", "content": "  hi  " },
        ]);
        let full = t.render(&messages, None).unwrap();
        assert_eq!(
            full,
            "<|im_start|>system\nBe brief.<|im_end|>\n<|im_start|>user\nhi<|im_end|>\n<|im_start|>assistant\n"
        );
        let f = t.framing(&messages, None).unwrap();
        assert_eq!(
            f.text,
            "<|im_start|>system\n<|im_end|>\n<|im_start|>user\n<|im_end|>\n<|im_start|>assistant\n"
        );
        assert_eq!(f.images, 0);
    }

    #[test]
    fn tools_and_tool_calls_are_framing() {
        let t = chatml();
        let tools = json!([{ "type": "function", "function": { "name": "get", "parameters": { "type": "object" } } }]);
        let messages = json!([
            { "role": "user", "content": "weather?" },
            { "role": "assistant", "content": "", "tool_calls": [
                { "type": "function", "function": { "name": "get", "arguments": { "city": "Oslo" } } }
            ] },
        ]);
        let f = t.framing(&messages, Some(&tools)).unwrap();
        // HF's tojson: ", " and ": " separators.
        assert!(
            f.text.contains(r#"{"function": {"name": "get", "parameters": {"type": "object"}}, "type": "function"}"#),
            "{}",
            f.text
        );
        assert!(f
            .text
            .contains(r#"<tool_call>{"arguments": {"city": "Oslo"}, "name": "get"}</tool_call>"#));
        assert!(!f.text.contains("weather"));
    }

    #[test]
    fn image_parts_are_counted_and_text_parts_marked() {
        let t = chatml();
        let messages = json!([{ "role": "user", "content": [
            { "type": "text", "text": "what is this?" },
            { "type": "image_url", "image_url": { "url": "data:..." } },
        ] }]);
        let f = t.framing(&messages, None).unwrap();
        assert_eq!(f.images, 1);
        assert!(!f.text.contains("what is this"));
    }

    #[test]
    fn templates_can_refuse_and_load_from_config() {
        let t = ChatTemplate::new(
            "{{ raise_exception('no system role') if messages[0].role == 'system' else 'ok' }}"
                .into(),
            String::new(),
            String::new(),
        )
        .unwrap();
        assert!(t
            .framing(&json!([{ "role": "system", "content": "x" }]), None)
            .is_err());
        assert!(t
            .framing(&json!([{ "role": "user", "content": "x" }]), None)
            .is_ok());

        let dir = std::env::temp_dir().join(format!("pt-tpl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = dir.join("tokenizer_config.json");
        std::fs::write(
            &cfg,
            json!({ "chat_template": [{ "name": "tool_use", "template": "T" }, { "name": "default", "template": "{{ bos_token }}D" }], "bos_token": { "content": "<s>" } }).to_string(),
        )
        .unwrap();
        let t = ChatTemplate::load(&cfg).unwrap();
        assert_eq!(t.render(&json!([]), None).unwrap(), "<s>D");
        std::fs::write(&cfg, "{}").unwrap();
        assert!(matches!(
            ChatTemplate::load(&cfg),
            Err(TemplateError::Missing { .. })
        ));
    }

    #[test]
    fn hf_tojson_indents_like_python() {
        let t = ChatTemplate::new(
            "{{ x | tojson(indent=2) }}".into(),
            String::new(),
            String::new(),
        )
        .unwrap();
        let out = t
            .env
            .get_template("chat")
            .unwrap()
            .render(minijinja::context! { x => JValue::from_serialize(json!({"a": [1, 2], "b": "<é>"})) })
            .unwrap();
        assert_eq!(
            out,
            "{\n  \"a\": [\n    1,\n    2\n  ],\n  \"b\": \"<é>\"\n}"
        );
    }
}
