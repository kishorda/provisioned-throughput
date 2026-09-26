# ADR-032: Count the chat template's framing by rendering it without message text

- **Status:** Accepted. Refines [ADR-028](ADR-028-input-token-counting.md).
- **Date:** 2026-09-26

## Context
ADR-028 counts message text with each model's tokenizer, but approximated the chat
template with a fixed `message_overhead` per message. An engine renders the model's Jinja
chat template before tokenizing. Besides role markers, the template adds a default system
prompt, every tool's JSON schema, earlier tool calls' arguments, and the generation
prompt. Image parts cost the vision encoder's tokens.

Measured with Qwen2.5's tokenizer and template, an agent turn with two tools and one tool
call is 361 tokens. The overhead count gave 45. Agent requests carry their tool list every
turn, so they were undercounted the most, and they are this product's main workload.

The options were:
- **Render the whole prompt and tokenize it**, as the engine does. Exact, but it throws away
  the per-message cache, so every agent turn re-tokenizes the whole conversation.
- **Render the template without message text.** Count what's left (the framing), and keep
  counting message text per message, with the cache.

## Decision
- **Load the model's template** (`chat_template` from `tokenizer_config.json`, or a
  `.jinja` file). By default it's the `tokenizer_config.json` beside `tokenizer.json`, as
  Hugging Face ships them. Rendering uses minijinja (pure Rust) with Hugging Face's
  context:
  - `messages`, `tools`, `add_generation_prompt`, `bos_token`, `eos_token`;
  - `raise_exception` and `strftime_now`;
  - Python string methods (`pycompat`);
  - HF's `tojson`: `", "` and `": "` separators, no HTML escaping, optional `indent`.
- **Framing.** The gateway renders the request's messages (with tool calls and image parts)
  and tools, with each message's text replaced by a placeholder. It then removes the
  placeholders and tokenizes the rest. The result is cached by its text, so an unchanged
  tool list is tokenized once. Message text is counted by itself, cached per message,
  within the inline budget (ADR-028). The framing is added to the first message's count,
  because tools and system defaults come first.
- **Images** cost `image_tokens` each (576, a placeholder per model), in the message that
  carries them.
- **Fallback.** A model without a template, or a request the template refuses
  (`raise_exception`), is counted with `message_overhead`. `GET /internal/v1/tokenizers`
  shows `chat_template` and `template_errors` per model.

## Consequences
- ✅ Counts match the rendered prompt. Against Qwen2.5, a plain chat, a multi-turn chat
  with a system prompt, and an agent turn with tools and a tool call each came to exactly
  the tokens of the fully rendered prompt: 37, 58, and 361. The overhead count gave 12, 51,
  and 45.
- ✅ The cache still works. A repeated request costs about 12–95 µs in a release build,
  mostly rendering the template. The first sight of a two-tool list costs about 1.5 ms.
- ⚠️ Text and template are tokenized apart. Where they meet, a merge the engine would make
  can split into two tokens (one per message at most).
- ⚠️ Templates that rewrite message text are counted by the original text. That is
  conservative: for example, dropping earlier reasoning from assistant turns is
  overcounted.
- ⚠️ Tool JSON keys are rendered in sorted order, not the request's order. The length is
  the same, so the count barely moves.
- ⚠️ `image_tokens` is one number per model. Real image costs depend on the image size and
  tiling.
