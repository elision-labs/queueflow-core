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
  version "0.3.0"
  license "MIT"

  livecheck do
    url :stable
    strategy :github_latest
  end

  on_macos do
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-apple-darwin.tar.gz"
      sha256 "6ae1a71233d30f50f6962a2875b6a956ae62ed52e014fc0a6ff1059363c77360"
    end
    # BEGIN x86_64-apple-darwin
    # Enabled by update-formula.sh once release.yml publishes this asset.
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-apple-darwin.tar.gz"
      sha256 "97590e238397b9ad2415d54017d18ba3e5f926178e2be0a15e0c91792811f1ae"
    end
    # END x86_64-apple-darwin
  end

  on_linux do
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "f57c662643e2bfcc405e692a4d6ff0397b808fcf9699b1d01d9c18b17d949e3c"
    end
    # BEGIN aarch64-unknown-linux-gnu
    # Enabled by update-formula.sh once release.yml publishes this asset.
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "999a507f17146a53aaeb07806256d7ee8b0fe044fccfc85da6540457ae0e27a9"
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
