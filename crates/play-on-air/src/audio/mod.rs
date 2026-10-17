//! Audio buffers and lossless encode helpers (FLAC / WAV).

pub mod flac;
pub mod pcm;
pub mod wav;

pub(crate) use flac::LIVE_ARENA_BYTES;
pub use flac::{FLAC_BLOCK_SIZE, LiveFlacEncoder, encode_pcm_i16_to_flac, round_up_to_flac_blocks};
pub use pcm::PcmRing;
pub use wav::{continuous_wav_header, encode_pcm_i16_to_wav, wav_header};
