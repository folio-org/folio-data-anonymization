The `tamboui-toolkit` bundled here is based on `main` as of May 6, 2026, with the following two additions:

- https://github.com/tamboui/tamboui/pull/326
- https://github.com/tamboui/tamboui/pull/331

We are bundling it directly to avoid waiting on these PRs to be merged. Once they are merged, we should be
safe to switch to the official `tamboui-toolkit` package and remove this bundled version.

There is also some custom code included here that is proposed for inclusion in the upstream library, so once
these PRs are merged, we should be able to remove our custom implementations as well:

- https://github.com/tamboui/tamboui/pull/327
- https://github.com/tamboui/tamboui/pull/330
