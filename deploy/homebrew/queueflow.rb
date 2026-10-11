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
  version "0.2.0"
  license "MIT"

  livecheck do
    url :stable
    strategy :github_latest
  end

  on_macos do
    on_arm do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-apple-darwin.tar.gz"
      sha256 "6e5002cc79c6f53c49e1504525ce1a9a86d73c4446164eb4c497c97cfb0e3e3f"
    end
    # BEGIN x86_64-apple-darwin
    # Enabled by update-formula.sh once release.yml publishes this asset.
    # on_intel do
    #   url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-apple-darwin.tar.gz"
    #   sha256 "0000000000000000000000000000000000000000000000000000000000000000"
    # end
    # END x86_64-apple-darwin
  end

  on_linux do
    on_intel do
      url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "6cb93405b41b3b09bee1edb8ff38bc8a3d514abdad4c7721973cd31f7be7e55c"
    end
    # BEGIN aarch64-unknown-linux-gnu
    # Enabled by update-formula.sh once release.yml publishes this asset.
    # on_arm do
    #   url "https://github.com/elision-labs/queueflow-core/releases/download/v#{version}/queueflow-aarch64-unknown-linux-gnu.tar.gz"
    #   sha256 "0000000000000000000000000000000000000000000000000000000000000000"
    # end
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
