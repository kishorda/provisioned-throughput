# ADR-036: Price inline images by their size and the model's tiling

- **Status:** Accepted. Refines [ADR-032](ADR-032-chat-template-token-counts.md).
- **Date:** 2026-09-26

## Context
ADR-032 counted every image part at a flat `image_tokens` per model. Vision encoders cut an
image into fixed-size tiles and spend a fixed number of tokens on each, usually plus a
thumbnail of the whole image. With Llama-4-style tiling (336 px tiles, 144 tokens each, up
to 16), a small image costs 288 tokens and a large one up to 2,448. So a flat number is
wrong for most images.

## Decision
- **Tiling config.** A tokenizer spec can describe its model's tiling as
  `image = { tile_px, tokens_per_tile, max_tiles, base_tokens }`.
- **Cost.** An image costs `base_tokens + tiles × tokens_per_tile`. Tiles are
  `⌈w / tile⌉ × ⌈h / tile⌉`, after scaling a large image down, keeping its aspect ratio,
  until it fits `max_tiles`. `detail: "low"` costs the base tokens alone.
- **Image size.** For images sent inline (`data:` URLs), the gateway reads the width and
  height from the header bytes: PNG, GIF, JPEG (the first start-of-frame marker, within the
  first 256 KB), and WebP (VP8, VP8L, VP8X). It never decodes pixels.
- **Fallback.** Remote image URLs, unreadable headers, and models without tiling cost the
  flat `image_tokens`. The gateway doesn't fetch images.

## Consequences
- ✅ Inline images are counted close to what the encoder uses, in microseconds.
- ⚠️ Each model family resizes differently: some snap to a grid of allowed aspect ratios,
  some pad. The downscale here is an approximation, and the tiling values are placeholders
  until calibrated per model.
- ⚠️ Remote images still cost the flat number. Settlement uses the engine's counts, so only
  the admission estimate is affected.
