---
content_title: Download nchain Source
---

To download the nchain source code, clone the `nchain` repo and its submodules. It is adviced to create a home `nchain` folder first and download all the nchain related software there:

```sh
mkdir -p ~/nchain && cd ~/nchain
git clone --recursive https://github.com/nchain/nchain.git
```

## Update Submodules

If a repository is cloned without the `--recursive` flag, the submodules *must* be updated before starting the build process:

```sh
cd ~/nchain/nchain
git submodule update --init --recursive
```

## Pull Changes

When pulling changes, especially after switching branches, the submodules *must* also be updated. This can be achieved with the `git submodule` command as above, or using `git pull` directly:

```sh
[git checkout <branch>]  (optional)
git pull --recurse-submodules
```

[[info | What's Next?]]
| [Build nchain binaries](02_build-nchain-binaries.md)
