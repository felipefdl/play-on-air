//! DMAP (Digital Media Access Protocol) parser.
//!
//! Parses the binary TLV format used by AirPlay for track metadata.
//! Format: 4-byte ASCII tag + 4-byte BE length + data.

/// Now-playing track metadata, from a DMAP `mlit` (SET_PARAMETER
/// `application/x-dmap-tagged`) or an `updateMRNowPlayingInfo` command.
///
/// Every field is optional: senders omit what they do not know, and an
/// `updateMRNowPlayingInfo` with `mergePolicy = update` carries only changed keys.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrackMetadata {
    /// Track title (`minm` / `title`).
    pub title: Option<String>,
    /// Artist name (`asar` / `artist`).
    pub artist: Option<String>,
    /// Album name (`asal` / `album`).
    pub album: Option<String>,
    /// Genre (`asgn`).
    pub genre: Option<String>,
    /// Composer (`ascp`).
    pub composer: Option<String>,
    /// Duration in milliseconds (`astm` / `duration`).
    pub duration_ms: Option<u32>,
    /// Track number (`astn` / `trackNumber`).
    pub track_number: Option<u16>,
    /// Track count (`astc` / `totalTracks`).
    pub track_count: Option<u16>,
    /// Disc number (`asdn`).
    pub disc_number: Option<u16>,
    /// Disc count (`asdc`).
    pub disc_count: Option<u16>,
    /// Whether the item is a stream (`asdk` = 1).
    pub is_stream: Option<bool>,
    /// Sender item id (`miid`).
    pub item_id: Option<u32>,
    /// Persistent item id (`mper`).
    pub persistent_id: Option<u64>,
}

impl TrackMetadata {
    /// Parse a DMAP `mlit` container into structured metadata.
    ///
    /// Unknown tags are skipped; a truncated or over-long item ends the parse with
    /// whatever was read before it.
    pub(crate) fn from_dmap(data: &[u8]) -> Self {
        let mut meta = Self::default();
        // Skip the 8-byte `mlit` container header.
        let mut rest = data.get(8..).unwrap_or_default();
        while let Some((tag, len_bytes, tail)) = rest
            .split_first_chunk::<4>()
            .and_then(|(tag, tail)| tail.split_first_chunk::<4>().map(|(len, tail)| (tag, len, tail)))
        {
            let len = u32::from_be_bytes(*len_bytes) as usize;
            let Some((chunk, after)) = tail.split_at_checked(len) else {
                break;
            };
            let as_str = || std::str::from_utf8(chunk).ok().map(String::from);
            let as_u64 = || chunk.try_into().ok().map(u64::from_be_bytes);
            let as_u32 = || chunk.try_into().ok().map(u32::from_be_bytes);
            let as_u16 = || chunk.try_into().ok().map(u16::from_be_bytes);
            match tag {
                b"minm" => meta.title = as_str(),
                b"asar" => meta.artist = as_str(),
                b"asal" => meta.album = as_str(),
                b"asgn" => meta.genre = as_str(),
                b"ascp" => meta.composer = as_str(),
                b"astm" => meta.duration_ms = as_u32(),
                b"astn" => meta.track_number = as_u16(),
                b"astc" => meta.track_count = as_u16(),
                b"asdn" => meta.disc_number = as_u16(),
                b"asdc" => meta.disc_count = as_u16(),
                b"asdk" => meta.is_stream = chunk.first().map(|kind| *kind == 1),
                b"miid" => meta.item_id = as_u32(),
                b"mper" => meta.persistent_id = as_u64(),
                _ => {
                    tracing::trace!(tag = %String::from_utf8_lossy(tag), len, "DMAP: unknown tag");
                }
            }
            rest = after;
        }
        tracing::debug!(?meta, "Track metadata parsed");
        meta
    }

    /// Overlay every field `update` carries onto `self` (`mergePolicy = update`).
    pub(crate) fn merge(&mut self, update: Self) {
        fn take<T>(slot: &mut Option<T>, value: Option<T>) {
            if value.is_some() {
                *slot = value;
            }
        }
        take(&mut self.title, update.title);
        take(&mut self.artist, update.artist);
        take(&mut self.album, update.album);
        take(&mut self.genre, update.genre);
        take(&mut self.composer, update.composer);
        take(&mut self.duration_ms, update.duration_ms);
        take(&mut self.track_number, update.track_number);
        take(&mut self.track_count, update.track_count);
        take(&mut self.disc_number, update.disc_number);
        take(&mut self.disc_count, update.disc_count);
        take(&mut self.is_stream, update.is_stream);
        take(&mut self.item_id, update.item_id);
        take(&mut self.persistent_id, update.persistent_id);
    }
}

#[cfg(test)]
mod tests {
    use super::TrackMetadata;

    const DMAP_FULL: &[u8] = &[
        0x6d, 0x6c, 0x69, 0x74, 0x00, 0x00, 0x00, 0x57, 0x6d, 0x69, 0x6b, 0x64, 0x00, 0x00, 0x00, 0x01, 0x02, 0x6d,
        0x69, 0x6e, 0x6d, 0x00, 0x00, 0x00, 0x11, 0x42, 0x6f, 0x68, 0x65, 0x6d, 0x69, 0x61, 0x6e, 0x20, 0x52, 0x68,
        0x61, 0x70, 0x73, 0x6f, 0x64, 0x79, 0x61, 0x73, 0x61, 0x72, 0x00, 0x00, 0x00, 0x05, 0x51, 0x75, 0x65, 0x65,
        0x6e, 0x61, 0x73, 0x61, 0x6c, 0x00, 0x00, 0x00, 0x14, 0x41, 0x20, 0x4e, 0x69, 0x67, 0x68, 0x74, 0x20, 0x61,
        0x74, 0x20, 0x74, 0x68, 0x65, 0x20, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x61, 0x73, 0x67, 0x6e, 0x00, 0x00, 0x00,
        0x04, 0x52, 0x6f, 0x63, 0x6b,
    ];

    #[test]
    fn dmap_parse_full() {
        let meta = TrackMetadata::from_dmap(DMAP_FULL);
        assert_eq!(meta.title.as_deref(), Some("Bohemian Rhapsody"));
        assert_eq!(meta.artist.as_deref(), Some("Queen"));
        assert_eq!(meta.album.as_deref(), Some("A Night at the Opera"));
        assert_eq!(meta.genre.as_deref(), Some("Rock"));
    }

    fn item(tag: &[u8; 4], value: &[u8]) -> Vec<u8> {
        let mut out = tag.to_vec();
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
        out
    }

    /// Spec 5 tag set: composer, counts, disc number (`asdn`, not `asdk`), stream flag, ids.
    #[test]
    fn dmap_parse_spec_tags() {
        let items = [
            item(b"ascp", b"Mercury"),
            item(b"astm", &354_000_u32.to_be_bytes()),
            item(b"astn", &11_u16.to_be_bytes()),
            item(b"astc", &12_u16.to_be_bytes()),
            item(b"asdn", &1_u16.to_be_bytes()),
            item(b"asdc", &2_u16.to_be_bytes()),
            item(b"asdk", &[1]),
            item(b"miid", &77_u32.to_be_bytes()),
            item(b"mper", &0x0102_0304_0506_0708_u64.to_be_bytes()),
        ]
        .concat();
        let data = item(b"mlit", &items);
        let meta = TrackMetadata::from_dmap(&data);
        assert_eq!(meta.composer.as_deref(), Some("Mercury"));
        assert_eq!(meta.duration_ms, Some(354_000));
        assert_eq!(meta.track_number, Some(11));
        assert_eq!(meta.track_count, Some(12));
        assert_eq!(meta.disc_number, Some(1));
        assert_eq!(meta.disc_count, Some(2));
        assert_eq!(meta.is_stream, Some(true));
        assert_eq!(meta.item_id, Some(77));
        assert_eq!(meta.persistent_id, Some(0x0102_0304_0506_0708));
    }

    #[test]
    fn merge_overlays_present_fields_only() {
        let mut base = TrackMetadata {
            title: Some("Old".into()),
            artist: Some("Band".into()),
            ..TrackMetadata::default()
        };
        base.merge(TrackMetadata {
            title: Some("New".into()),
            album: Some("Record".into()),
            ..TrackMetadata::default()
        });
        assert_eq!(base.title.as_deref(), Some("New"));
        assert_eq!(base.artist.as_deref(), Some("Band"));
        assert_eq!(base.album.as_deref(), Some("Record"));
    }

    #[test]
    fn dmap_parse_title_only() {
        let data: &[u8] = &[
            0x6d, 0x6c, 0x69, 0x74, 0x00, 0x00, 0x00, 0x0d, 0x6d, 0x69, 0x6e, 0x6d, 0x00, 0x00, 0x00, 0x05, 0x48, 0x65,
            0x6c, 0x6c, 0x6f,
        ];
        let meta = TrackMetadata::from_dmap(data);
        assert_eq!(meta.title.as_deref(), Some("Hello"));
        assert_eq!(meta.artist, None);
    }

    #[test]
    fn dmap_parse_empty() {
        let meta = TrackMetadata::from_dmap(&[]);
        assert_eq!(meta.title, None);
    }

    #[test]
    fn dmap_parse_truncated() {
        let meta = TrackMetadata::from_dmap(&DMAP_FULL[..8]);
        assert_eq!(meta.title, None);
    }

    #[test]
    fn dmap_parse_corrupt_length() {
        let data: &[u8] = &[
            0x6d, 0x6c, 0x69, 0x74, 0x00, 0x00, 0x00, 0x0d, 0x6d, 0x69, 0x6e, 0x6d, 0x00, 0x00, 0xff, 0xff, 0x48, 0x65,
            0x6c, 0x6c, 0x6f,
        ];
        let meta = TrackMetadata::from_dmap(data);
        assert_eq!(meta.title, None);
    }
}
