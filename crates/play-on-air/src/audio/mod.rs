//! Audio buffers and lossless encode helpers (FLAC / WAV).

pub mod flac;
pub mod pcm;
pub mod wav;

pub use flac::{
  FLAC_BLOCK_SIZE, FlacByteSink, encode_i16_block_to_frame, encode_pcm_i16_to_flac, live_frame_buf,
  live_stream_header_bytes, live_stream_info, round_up_to_flac_blocks, verified_encoder_config,
};
pub use pcm::PcmRing;
pub use wav::{continuous_wav_header, encode_pcm_i16_to_wav, wav_header};
