# Changelog

## [0.3.2](https://github.com/DIRACGrid/signurlarity/compare/v0.3.1...v0.3.2) (2026-11-24)


### Features

* download_file client ([#55](https://github.com/DIRACGrid/signurlarity/issues/55)) ([ded8268](https://github.com/DIRACGrid/signurlarity/commit/ded8268ca01f60173fca3ba2f324f450a826d3ae))


### Bug Fixes

* do not use private httpx._config for default limits ([#67](https://github.com/DIRACGrid/signurlarity/issues/67)) ([a0d9333](https://github.com/DIRACGrid/signurlarity/commit/a0d93337d14d887433ebb8e5ade2553369218934))
* make renovate custom manager match conftest.py files ([#58](https://github.com/DIRACGrid/signurlarity/issues/58)) ([35fba3d](https://github.com/DIRACGrid/signurlarity/commit/35fba3deade990431d852981627c43778a89415d))
* migrate from httpx to httpx2 ([#48](https://github.com/DIRACGrid/signurlarity/issues/48)) ([3d4050e](https://github.com/DIRACGrid/signurlarity/commit/3d4050e947aaa6447b2f023ea6578d83f45ed70c))
* update httpcore logger name to httpcore2 ([#51](https://github.com/DIRACGrid/signurlarity/issues/51)) ([8d9421c](https://github.com/DIRACGrid/signurlarity/commit/8d9421c83228037472cd3696a82e77c6cf7363c6))

## [0.3.1](https://github.com/DIRACGrid/signurlarity/compare/v0.3.0...v0.3.1) (2026-06-18)


### Bug Fixes

* percent-encode object keys in header-signed request paths ([#39](https://github.com/DIRACGrid/signurlarity/issues/39)) ([c8fc80c](https://github.com/DIRACGrid/signurlarity/commit/c8fc80c546902373476867cba6ec58dd43286e9b))
* percent-encode the x-amz-copy-source header value ([#43](https://github.com/DIRACGrid/signurlarity/issues/43)) ([4c537aa](https://github.com/DIRACGrid/signurlarity/commit/4c537aa8739e49c4e043dbe1ef8efe7360d45d12))
* raise NoSuchKeyError on 404 for object operations ([#42](https://github.com/DIRACGrid/signurlarity/issues/42)) ([2536dbc](https://github.com/DIRACGrid/signurlarity/commit/2536dbc3e2e2b182f7479b048585eecd2b3d1eba))

## [0.3.0](https://github.com/DIRACGrid/signurlarity/compare/v0.2.2...v0.3.0) (2026-05-29)


### Features

* add delete bucket method in client ([#32](https://github.com/DIRACGrid/signurlarity/issues/32)) ([da76128](https://github.com/DIRACGrid/signurlarity/commit/da76128655244c85d6c4d53b535c2f2a969ad1cf))


### Bug Fixes

* correct v4 headers. See https://github.com/seaweedfs/seaweedfs/pull/9121 ([#36](https://github.com/DIRACGrid/signurlarity/issues/36)) ([3646065](https://github.com/DIRACGrid/signurlarity/commit/3646065c483d5ea29862e4f090edd6f8a8374a16))

## [0.2.2](https://github.com/DIRACGrid/signurlarity/compare/v0.2.1...v0.2.2) (2026-04-16)


### Bug Fixes

* more ACL fixes ([#30](https://github.com/DIRACGrid/signurlarity/issues/30)) ([bd29aa8](https://github.com/DIRACGrid/signurlarity/commit/bd29aa8cee2e237f27b9eb49434ec3b740b7aade))

## [0.2.1](https://github.com/DIRACGrid/signurlarity/compare/v0.2.0...v0.2.1) (2026-03-30)


### Bug Fixes

* fix ACL ([#27](https://github.com/DIRACGrid/signurlarity/issues/27)) ([a2c302e](https://github.com/DIRACGrid/signurlarity/commit/a2c302ed36afd5b745009a5f671fb4bd13880bf3))

## [0.2.0](https://github.com/DIRACGrid/signurlarity/compare/v0.1.0...v0.2.0) (2026-03-27)


### Features

* implement upload_file,copy_object,list_objects,put_object ([#24](https://github.com/DIRACGrid/signurlarity/issues/24)) ([6548b84](https://github.com/DIRACGrid/signurlarity/commit/6548b84003431dbbd3b4f683f9d8a67ba7b5e29d))

## [0.1.0](https://github.com/DIRACGrid/signurlarity/compare/v0.0.1...v0.1.0) (2026-03-18)


### Features

* Implement delete_objects method ([#18](https://github.com/DIRACGrid/signurlarity/issues/18)) ([8ef9e5c](https://github.com/DIRACGrid/signurlarity/commit/8ef9e5c977f4c1bac4ba9c13abbcc9892276311a))


### Bug Fixes

* Add missing `cryptography` package dependency ([#23](https://github.com/DIRACGrid/signurlarity/issues/23)) ([34ddf99](https://github.com/DIRACGrid/signurlarity/commit/34ddf9956174600c8baf76adce492a10ecf813fd))
