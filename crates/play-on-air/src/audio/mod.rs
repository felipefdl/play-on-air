//! Audio buffers and lossless encode helpers (FLAC / WAV).

pub mod decode_path;
pub mod flac;
pub mod pcm;
pub mod wav;

pub use flac::encode_pcm_i16_to_flac;
pub use pcm::PcmRing;
pub use wav::{continuous_wav_header, encode_pcm_i16_to_wav, wav_header};
