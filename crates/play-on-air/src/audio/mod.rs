//! Audio buffers and lossless encode helpers (FLAC / WAV).

pub mod flac;
pub mod pcm;
pub mod wav;

pub(crate) use flac::LIVE_ARENA_BYTES;
pub use flac::{FLAC_BLOCK_SIZE, LiveFlacEncoder, round_up_to_flac_blocks};
pub use pcm::PcmRing;
pub use wav::{continuous_wav_header, wav_header};
