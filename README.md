# SignURLarity

Lightweight library to presign URLs compatible with what boto does.

## Installation

```bash
pip install signurlarity
```

## tests

[installation pixi](https://pixi.sh/latest/advanced/installation/)

```bash
pixi run pytest # add any pytest option you want
```

## pre-commit

SignURLarity uses [`pre-commit`](https://pre-commit.com/) to format code and check for issues.
The easiest way to use `pre-commit` is to run the following after cloning:

```bash
pixi run pre-commit install
```

This will result in pre-commit being ran automatically each time you run `git commit`.
If you want to explicitly run pre-commit you can use:

```bash
pixi run pre-commit # (1)!
pixi run pre-commit --all-files # (2)!
```

1. Runs `pre-commit` only for files which are uncommitted or which have been changed.
2. Runs `pre-commit` for all files even if you haven't changed them.
