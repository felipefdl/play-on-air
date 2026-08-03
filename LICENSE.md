# MIT License

Copyright (c) 2026 Felipe Lima

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

---

## Third-party components

### vendor/shairplay

`vendor/shairplay` is a fork of the [`shairplay`](https://crates.io/crates/shairplay) Rust crate (upstream project: [metaneutrons/shairplay-rust](https://github.com/metaneutrons/shairplay-rust)), licensed **LGPL-3.0-or-later**.

The compiled PlayOnAir binary and Docker images on GHCR statically link this library. Its complete corresponding source is included in this repository under `vendor/shairplay/`, which satisfies the LGPL source-availability obligations for the combined work. The license text for that component is `vendor/shairplay/LICENSE`.

All other first-party code and documentation in this repository remain under the MIT License above.
