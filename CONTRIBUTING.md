# Contributing to Events Caravan

Thank you for considering a contribution! Events Caravan is maintained by
[Sagynysh Baitursinov](https://github.com/SagynyshBaitursinov). Issues, discussions, and pull requests are welcome.

## Ways to contribute

- **Report a bug** — open an issue using the bug report template. Include reproduction steps and versions.
- **Propose a feature or improvement** — open an issue first so we can discuss the design before you invest time in
  code. Proposals are evaluated against the project's [philosophy principles](README.md#philosophy-in-principles):
  horizontal scalability, simple and cheap technology features, one write destination, eventual consistency.
- **Improve documentation** — README fixes, clearer explanations, and diagram corrections are always welcome as
  direct pull requests.
- **Implement an adapter** — the core is technology-agnostic; adapters for other databases and brokers that honor the
  library's contracts are great contributions.
- [TODOs from the author](TODOs.md) with further plan of developing the library.

## Development setup

### Prerequisites

- **Java 25** (Temurin is what CI uses)
- **Docker** — the integration test environment emulates AWS locally (DynamoDB, SNS, SQS, Lambda) via
  [floci](https://hub.docker.com/r/floci/floci) and Docker Compose
- **AWS CLI** — used by the local environment scripts to provision the emulated infrastructure

No AWS account is needed; everything runs locally against the emulator with dummy credentials.

### Build and test

```bash
# Full test suite, exactly as CI runs it:
# starts the local AWS environment, runs all tests, tears the environment down
./local/test

# Or, step by step:
./local/env-up          # start emulator, create tables/topics/queues/lambda
./local/execute-tests   # ./mvnw test
./local/env-down        # stop and clean up
```

A plain `./mvnw test` also works while the local environment is up.

## Pull request guidelines

1. **Open an issue first** for anything beyond a trivial fix, so the approach can be agreed on.
2. **Branch from `main`** and keep the PR focused on a single change.
3. **Use [Conventional Commits](https://www.conventionalcommits.org/)** for commit messages and PR titles
   (`feat:`, `fix:`, `docs:`, `test:`, `chore:` …). Releases and the changelog are automated with
   release-please, which reads these prefixes to determine version bumps.
4. **Add or update tests** for behavior changes. The `test` status check must pass before merging.
5. **Match the existing code style** of the module you are touching.
6. **Update documentation** (README, Javadoc) when behavior or configuration changes.

The maintainer reviews every pull request. Reviews are done on a best-effort basis — please be patient, and feel free
to ping the PR after a couple of weeks of silence.

## Reporting security issues

Please do **not** open a public issue for security vulnerabilities — see [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are
expected to uphold it.

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
