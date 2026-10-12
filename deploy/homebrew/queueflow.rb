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
  version "0.6.0"
  license "MIT"

  livecheck do
    url :stable
    strategy :github_latest
  end

  on_macos do
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-apple-darwin.tar.gz"
      sha256 "eaa4b01b547e3a2828bb5cea1d550f256381d77faa93d3573ebb49dada927876"
    end
    # BEGIN x86_64-apple-darwin
    # Enabled by update-formula.sh once release.yml publishes this asset.
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-apple-darwin.tar.gz"
      sha256 "f3c1cdfaeaaeb8782883138f93a1e70222403fb8270ff704eceaadf382d19b21"
    end
    # END x86_64-apple-darwin
  end

  on_linux do
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "82751db6cdcb1e2f3e15f206e82ab7f21d636036e6629e11279ffe0039bbc5ae"
    end
    # BEGIN aarch64-unknown-linux-gnu
    # Enabled by update-formula.sh once release.yml publishes this asset.
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "4674fffc9aecff7e231f494363c5b02b3b87c18c91114fde24bf202b41869411"
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
