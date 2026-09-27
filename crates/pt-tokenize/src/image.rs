//! What an image part costs, from its size (ADR-036).
//!
//! Vision encoders cut an image into tiles and spend a fixed number of tokens on each, often
//! plus a downscaled thumbnail of the whole image. A flat per-image cost undercounts large
//! images and overcounts small ones. For images sent inline (`data:` URLs), the gateway
//! reads the width and height from the image header (PNG, JPEG, GIF, WebP), without decoding
//! the pixels, and applies the model's tiling. Remote images (the engine fetches them) and
//! models without a tiling description cost the flat `image_tokens`.

use base64::Engine;
use serde::Deserialize;
use serde_json::Value;

/// How a model's vision encoder tiles an image.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageTiling {
    /// Tile edge in pixels.
    pub tile_px: u32,
    pub tokens_per_tile: u64,
    /// Larger images are downscaled until they fit in this many tiles.
    pub max_tiles: u32,
    /// Tokens for the whole-image thumbnail, added to every image, and all a
    /// `detail: "low"` image costs.
    #[serde(default)]
    pub base_tokens: u64,
}

impl ImageTiling {
    /// Tokens for a `width` × `height` image.
    pub fn tokens(&self, width: u32, height: u32) -> u64 {
        let tile = f64::from(self.tile_px.max(1));
        let (mut w, mut h) = (f64::from(width.max(1)), f64::from(height.max(1)));
        let tiles = |w: f64, h: f64| (w / tile).ceil() * (h / tile).ceil();
        // Downscale, keeping the aspect ratio, until the tiles fit.
        let max = f64::from(self.max_tiles.max(1));
        while tiles(w, h) > max {
            w *= 0.9;
            h *= 0.9;
        }
        self.base_tokens + tiles(w, h) as u64 * self.tokens_per_tile
    }
}

/// Tokens for one message content part, if it's an image. `flat` is the cost when the size
/// can't be known.
pub fn part_tokens(part: &Value, tiling: Option<&ImageTiling>, flat: u64) -> Option<u64> {
    let kind = part.get("type").and_then(Value::as_str)?;
    let url = match kind {
        "image_url" => part["image_url"]["url"]
            .as_str()
            .or(part["image_url"].as_str()),
        "image" | "input_image" => part["image_url"]
            .as_str()
            .or(part["url"].as_str())
            .or(part["image"].as_str()),
        _ => return None,
    };
    let Some(tiling) = tiling else {
        return Some(flat);
    };
    let detail = part["image_url"]["detail"]
        .as_str()
        .or(part["detail"].as_str());
    if detail == Some("low") {
        return Some(tiling.base_tokens.max(tiling.tokens_per_tile));
    }
    Some(
        url.and_then(data_url_dimensions)
            .map_or(flat, |(w, h)| tiling.tokens(w, h)),
    )
}

/// Width and height of a base64 `data:` image, from its header.
pub fn data_url_dimensions(url: &str) -> Option<(u32, u32)> {
    let rest = url.strip_prefix("data:")?;
    let (meta, payload) = rest.split_once(',')?;
    if !meta.ends_with(";base64") {
        return None;
    }
    // Headers are near the start; JPEG's frame header may follow EXIF data, so read up to
    // 256 KB. Decode whole 4-character groups only.
    let take = payload.len().min(256 * 1024 / 3 * 4) / 4 * 4;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload[..take].trim_end_matches('='))
        .or_else(|_| {
            base64::engine::general_purpose::STANDARD_NO_PAD
                .decode(payload[..take].trim_end_matches('='))
        })
        .ok()?;
    dimensions(&bytes)
}

/// Width and height from PNG, GIF, JPEG, or WebP header bytes.
pub fn dimensions(b: &[u8]) -> Option<(u32, u32)> {
    let be32 = |i: usize| Some(u32::from_be_bytes(b.get(i..i + 4)?.try_into().ok()?));
    let le16 = |i: usize| {
        Some(u32::from(u16::from_le_bytes(
            b.get(i..i + 2)?.try_into().ok()?,
        )))
    };
    let be16 = |i: usize| {
        Some(u32::from(u16::from_be_bytes(
            b.get(i..i + 2)?.try_into().ok()?,
        )))
    };
    let le24 = |i: usize| {
        let s = b.get(i..i + 3)?;
        Some(u32::from(s[0]) | u32::from(s[1]) << 8 | u32::from(s[2]) << 16)
    };
    if b.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some((be32(16)?, be32(20)?));
    }
    if b.starts_with(b"GIF87a") || b.starts_with(b"GIF89a") {
        return Some((le16(6)?, le16(8)?));
    }
    if b.starts_with(b"RIFF") && b.get(8..12) == Some(b"WEBP") {
        return match b.get(12..16)? {
            b"VP8X" => Some((le24(24)? + 1, le24(27)? + 1)),
            b"VP8 " => Some((le16(26)? & 0x3fff, le16(28)? & 0x3fff)),
            b"VP8L" => {
                let bits = u32::from_le_bytes(b.get(21..25)?.try_into().ok()?);
                Some(((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1))
            }
            _ => None,
        };
    }
    if b.starts_with(&[0xff, 0xd8]) {
        // Walk the JPEG segments to the first start-of-frame marker.
        let mut i = 2;
        while i + 4 <= b.len() {
            if b[i] != 0xff {
                return None;
            }
            let marker = b[i + 1];
            if marker == 0xd8 || (0xd0..=0xd7).contains(&marker) || marker == 0x01 {
                i += 2;
                continue;
            }
            let len = be16(i + 2)? as usize;
            let sof = matches!(marker, 0xc0..=0xcf) && !matches!(marker, 0xc4 | 0xc8 | 0xcc);
            if sof {
                return Some((be16(i + 7)?, be16(i + 5)?));
            }
            i += 2 + len;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn png(w: u32, h: u32) -> Vec<u8> {
        let mut b = b"\x89PNG\r\n\x1a\n\0\0\0\x0dIHDR".to_vec();
        b.extend(w.to_be_bytes());
        b.extend(h.to_be_bytes());
        b.extend([8, 2, 0, 0, 0]);
        b
    }

    fn data_url(mime: &str, bytes: &[u8]) -> String {
        format!(
            "data:{mime};base64,{}",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        )
    }

    #[test]
    fn reads_sizes_from_headers() {
        assert_eq!(dimensions(&png(1920, 1080)), Some((1920, 1080)));
        let mut gif = b"GIF89a".to_vec();
        gif.extend([0x40, 0x01, 0xf0, 0x00]);
        assert_eq!(dimensions(&gif), Some((320, 240)));
        // JPEG: SOI, an APP0 segment, then SOF0 with height 600 and width 800.
        let mut jpeg = vec![0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0, 0];
        jpeg.extend([0xff, 0xc0, 0x00, 0x11, 0x08, 0x02, 0x58, 0x03, 0x20, 0x03]);
        assert_eq!(dimensions(&jpeg), Some((800, 600)));
        // WebP extended: canvas 1024 × 768, stored minus one.
        let mut webp = b"RIFF\0\0\0\0WEBPVP8X\x0a\0\0\0\0\0\0\0".to_vec();
        webp.extend([0xff, 0x03, 0x00, 0xff, 0x02, 0x00]);
        assert_eq!(dimensions(&webp), Some((1024, 768)));
        assert_eq!(dimensions(b"not an image"), None);
        assert_eq!(
            data_url_dimensions(&data_url("image/png", &png(640, 480))),
            Some((640, 480))
        );
        assert_eq!(data_url_dimensions("https://example.com/cat.png"), None);
    }

    /// Llama-4-style tiling: 336 px tiles of 144 tokens, up to 16, plus a thumbnail.
    const TILING: ImageTiling = ImageTiling {
        tile_px: 336,
        tokens_per_tile: 144,
        max_tiles: 16,
        base_tokens: 144,
    };

    #[test]
    fn tiles_by_size_and_caps_large_images() {
        assert_eq!(TILING.tokens(300, 300), 144 + 144);
        assert_eq!(TILING.tokens(672, 336), 144 + 2 * 144);
        // 4K is downscaled to fit 16 tiles.
        let big = TILING.tokens(3840, 2160);
        assert!((144 + 12 * 144..=144 + 16 * 144).contains(&big), "{big}");
    }

    #[test]
    fn prices_parts() {
        let part = |url: &str, detail: Option<&str>| {
            let mut p = json!({ "type": "image_url", "image_url": { "url": url } });
            if let Some(d) = detail {
                p["image_url"]["detail"] = json!(d);
            }
            p
        };
        let small = data_url("image/png", &png(300, 300));
        assert_eq!(
            part_tokens(&part(&small, None), Some(&TILING), 576),
            Some(288)
        );
        assert_eq!(
            part_tokens(&part(&small, Some("low")), Some(&TILING), 576),
            Some(144)
        );
        // A remote image's size is unknown: the flat cost.
        assert_eq!(
            part_tokens(&part("https://x/y.png", None), Some(&TILING), 576),
            Some(576)
        );
        // No tiling for the model: the flat cost.
        assert_eq!(part_tokens(&part(&small, None), None, 576), Some(576));
        assert_eq!(
            part_tokens(&json!({ "type": "text", "text": "hi" }), None, 576),
            None
        );
    }
}
