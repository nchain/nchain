---
content_title: Build nchain Binaries
---

[[info | Shell Scripts]]
| The build script is one of various automated shell scripts provided in the nchain repository for building, installing, and optionally uninstalling the nchain software and its dependencies. They are available in the `nchain/scripts` folder.

The build script first installs all dependencies and then builds nchain. The script supports these [Operating Systems](../../index.md#supported-operating-systems). To run it, first change to the `~/nchain/nchain` folder, then launch the script:

```sh
cd ~/nchain/nchain
mkdir build && cd build
bash ../scripts/build/nchain_build.sh
```

The build process writes temporary content to the `nchain/build` folder. After building, the program binaries can be found at `nchain/build/programs`.
