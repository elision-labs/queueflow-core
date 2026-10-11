# Homebrew formula for the QueueFlow server/CLI binary.
#
# Lives in the tap repository elision-labs/homebrew-tap as Formula/queueflow.rb
# (this file is the source of truth; copy it there on each release, see
# README.md). It installs the prebuilt release tarballs built by
# .github/workflows/release.yml, named queueflow-<rust-target>.tar.gz, each
# containing the single `queueflow` binary.
#
# Regenerate version and checksums with: ./update-formula.sh v0.2.0
class Queueflow < Formula
  desc "PostgreSQL-native job queue and workflow engine (server + CLI)"
  homepage "https://queueflow.dev"
  version "0.5.0"
  license "MIT"

  livecheck do
    url :stable
    strategy :github_latest
  end

  on_macos do
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-apple-darwin.tar.gz"
      sha256 "deb986aead27bf4728119162f31bc89877283d18ea4ce75f96f20fd8a5ba8562"
    end
    # BEGIN x86_64-apple-darwin
    # Enabled by update-formula.sh once release.yml publishes this asset.
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-apple-darwin.tar.gz"
      sha256 "b26df65c9beb195abfe2cbe12b7f50abc5f34ba2c4b19631007a532401dcf21f"
    end
    # END x86_64-apple-darwin
  end

  on_linux do
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "fb401a3806e8f6eb073b8ad88f65ec59241381b026adaf6d964d080a2e1d431f"
    end
    # BEGIN aarch64-unknown-linux-gnu
    # Enabled by update-formula.sh once release.yml publishes this asset.
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "807fabe59803a2b847380d7a83c7d965491e32126bfdd5ce557b9251bd23e765"
    end
    # END aarch64-unknown-linux-gnu
  end

  def install
    bin.install "queueflow"
  end

  def caveats
    <<~EOS
      Start a server against any PostgreSQL 13+:
        export DATABASE_URL=postgres://user:pass@localhost:5432/db
        queueflow serve --dev --mode all          # local only; never --dev on a reachable host

      Production needs credentials (the server refuses to start without them):
        queueflow serve --mode all --api-keys "$(openssl rand -hex 24):acme" \\
                                   --worker-token "$(openssl rand -hex 24)"
    EOS
  end

  test do
    assert_match "queueflow #{version}", shell_output("#{bin}/queueflow --version")
    assert_match "serve", shell_output("#{bin}/queueflow --help")
  end
end
