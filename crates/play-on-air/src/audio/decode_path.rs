//! Decode-path tests: real AAC fixture (and post-decode PCM) through ring → FLAC.
//!
//! Production AP2 wire decode is performed by shairplay (buffered AAC / realtime ALAC).
//! These tests exercise the **shipped** post-decode path with:
//! - a real ADTS AAC fixture decoded by symphonia (honest AAC → PCM)
//! - post-decode PCM shaped like realtime ALAC after shairplay delivers f32

#[cfg(test)]
mod tests {
  use std::f32::consts::PI;
  use std::io::Cursor;
  use std::path::PathBuf;
  use std::sync::Arc;

  use symphonia::core::audio::sample::Sample;
  use symphonia::core::codecs::audio::AudioDecoderOptions;
  use symphonia::core::errors::Error as SymphoniaError;
  use symphonia::core::formats::FormatOptions;
  use symphonia::core::formats::probe::Hint;
  use symphonia::core::io::{MediaSourceStream, MediaSourceStreamOptions};
  use symphonia::core::meta::MetadataOptions;
  use symphonia_core::formats::TrackType;

  use crate::audio::{PcmRing, encode_pcm_i16_to_flac};

  fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
      .join("tests")
      .join("fixtures")
      .join(name)
  }

  /// Decode an ADTS AAC file to interleaved f32 PCM via symphonia 0.6 (real AAC path).
  fn decode_aac_adts_to_f32(path: &std::path::Path) -> (Vec<f32>, u32, u16) {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let mss = MediaSourceStream::new(Box::new(file), MediaSourceStreamOptions::default());
    let mut hint = Hint::new();
    let _hint = hint.with_extension("aac");

    let mut format = symphonia::default::get_probe()
      .probe(&hint, mss, FormatOptions::default(), MetadataOptions::default())
      .expect("probe AAC");

    let track = format.default_track(TrackType::Audio).expect("audio track");
    let track_id = track.id;
    let audio_params = track
      .codec_params
      .as_ref()
      .and_then(symphonia_core::codecs::CodecParameters::audio)
      .expect("audio codec params");
    let sample_rate = audio_params.sample_rate.expect("sample_rate");
    #[allow(
      clippy::redundant_closure_for_method_calls,
      reason = "Channels path is private in symphonia-core"
    )]
    let channel_count = audio_params.channels.as_ref().map_or(2, |ch| ch.count());
    let channels = u16::try_from(channel_count).unwrap_or(2);

    let mut decoder = symphonia::default::get_codecs()
      .make_audio_decoder(audio_params, &AudioDecoderOptions::default())
      .expect("aac decoder");

    let mut pcm = Vec::new();
    let mut frame_buf: Vec<f32> = Vec::new();
    while let Ok(Some(packet)) = format.next_packet() {
      if packet.track_id != track_id {
        continue;
      }
      match decoder.decode(&packet) {
        Ok(audio_buf) => {
          frame_buf.resize(audio_buf.samples_interleaved(), f32::MID);
          audio_buf.copy_to_slice_interleaved(&mut frame_buf);
          pcm.extend_from_slice(&frame_buf);
        },
        Err(SymphoniaError::DecodeError(_)) => {},
        Err(_) => break,
      }
    }

    assert!(!pcm.is_empty(), "decoded PCM must be non-empty");
    (pcm, sample_rate, channels.max(1))
  }

  /// Real AAC ADTS fixture → symphonia decode → [`PcmRing`] → FLAC → claxon.
  #[test]
  fn real_aac_fixture_decode_to_flac() {
    let path = fixture_path("sine_440_stereo.aac");
    assert!(path.is_file(), "fixture missing at {}", path.display());

    let (pcm_f32, sample_rate, channels) = decode_aac_adts_to_f32(&path);
    let frames = pcm_f32.len() / usize::from(channels);
    assert!(frames > 100, "expected a real decoded length, got {frames}");

    let ring = Arc::new(PcmRing::new(channels, frames.saturating_mul(2).max(1024)));
    ring.push_f32(&pcm_f32);

    let mut i16_buf = Vec::new();
    let got = ring.pop_i16(frames, &mut i16_buf);
    assert_eq!(got, frames);
    assert_eq!(i16_buf.len(), frames.saturating_mul(usize::from(channels)));

    let flac = encode_pcm_i16_to_flac(&i16_buf, channels, sample_rate).expect("encode");
    assert!(flac.len() > 42);
    assert_eq!(&flac[0..4], b"fLaC");

    let cursor = Cursor::new(&flac);
    let mut flac_reader = claxon::FlacReader::new(cursor).expect("claxon open");
    let info = flac_reader.streaminfo();
    assert_eq!(info.sample_rate, sample_rate);
    assert_eq!(info.channels, u32::from(channels));
    let decoded: Vec<i32> = flac_reader.samples().map(|r| r.expect("sample")).collect();
    assert_eq!(decoded.len(), i16_buf.len());
  }

  fn sine_f32(frames: usize, channels: u16, freq_hz: f32, sample_rate: u32, amplitude: f32) -> Vec<f32> {
    let ch = usize::from(channels);
    let mut out = Vec::with_capacity(frames.saturating_mul(ch));
    for n in 0..frames {
      let t = n as f32 / sample_rate as f32;
      let s = (2.0 * PI * freq_hz * t).sin() * amplitude;
      for _ in 0..ch {
        out.push(s);
      }
    }
    out
  }

  /// Post-decode realtime-style path: PCM as delivered by shairplay after ALAC.
  #[test]
  fn post_decode_realtime_pcm_to_flac() {
    let sample_rate = 48_000_u32;
    let channels = 2_u16;
    let frames = 4096_usize;
    let ring = Arc::new(PcmRing::new(channels, frames * 2));
    let pcm_f32 = sine_f32(frames, channels, 440.0, sample_rate, 0.5);
    ring.push_f32(&pcm_f32);

    let mut i16_buf = Vec::new();
    let got = ring.pop_i16(frames, &mut i16_buf);
    assert_eq!(got, frames);

    let flac = encode_pcm_i16_to_flac(&i16_buf, channels, sample_rate).expect("encode");
    assert!(flac.len() > 42);
    assert_eq!(&flac[0..4], b"fLaC");

    let cursor = Cursor::new(&flac);
    let flac_reader = claxon::FlacReader::new(cursor).expect("claxon open");
    let info = flac_reader.streaminfo();
    assert_eq!(info.sample_rate, sample_rate);
    assert_eq!(info.channels, u32::from(channels));
  }
}
