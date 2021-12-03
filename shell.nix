let
  # Last updated: 2/26/21. Update as necessary from https://status.nixos.org/...
  pkgs = import (fetchTarball("https://github.com/NixOS/nixpkgs/archive/660a7744c6557667366db5874742891aa1548620.tar.gz")) {};

  # Rolling updates, not deterministic.
  # pkgs = import (fetchTarball("channel:nixpkgs-unstable")) {};
in pkgs.mkShell {
  buildInputs = [
    pkgs.cargo
    pkgs.rustc
    pkgs.rustfmt
    pkgs.rustup
    pkgs.rust-analyzer
    pkgs.clippy

    # Necessary for the openssl-sys crate:
    pkgs.openssl
    pkgs.pkg-config

    pkgs.protobuf
  ];


  # See https://discourse.nixos.org/t/rust-src-not-found-and-other-misadventures-of-developing-rust-on-nixos/11570/3?u=samuela.
  RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";

}

