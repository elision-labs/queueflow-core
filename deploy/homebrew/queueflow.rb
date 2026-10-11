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
  version "0.4.0"
  license "MIT"

  livecheck do
    url :stable
    strategy :github_latest
  end

  on_macos do
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-apple-darwin.tar.gz"
      sha256 "360a121959bb85c4af4a67e977eb62b150cd8bb06b5fbdf0de5357465de0430c"
    end
    # BEGIN x86_64-apple-darwin
    # Enabled by update-formula.sh once release.yml publishes this asset.
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-apple-darwin.tar.gz"
      sha256 "d6b460ecc658ccf19c25696cafd35e97df19aad85031fcd0c2a53a34ab614bc7"
    end
    # END x86_64-apple-darwin
  end

  on_linux do
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "1dbc893421bb60ffbe69f122d4229e64ad7c57398c8b82751d61f8f95a51efad"
    end
    # BEGIN aarch64-unknown-linux-gnu
    # Enabled by update-formula.sh once release.yml publishes this asset.
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "d7328e7486a89722eaa34759bb8767990bba5361a90189e823e8c6ac4bd3f90d"
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
