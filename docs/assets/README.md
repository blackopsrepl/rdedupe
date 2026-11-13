# Assets

- `scan.png`, `cleanup.png`: rendered by `make screenshots` (`scripts/screenshots.py`) from real runs
  of the release binary on a generated photo fixture. The only change made to the output is that
  the fixture's temporary root is shown as `~`. Re-render them whenever the human output changes.
- `mascot.png`: the rdedupe hermit crab. It wears a filing crate (the quarantine) as its shell,
  holds two identical cards up for comparison, and checks them through a loupe. Generated with
  gpt-image-2 (request `ebbceebc-1067-4664-8b5e-692843dcd867`) on a flat magenta backdrop, keyed
  to alpha, trimmed to 800 px and palette-quantized with pngquant. The prompt is in
  `mascot-prompt.txt`.
