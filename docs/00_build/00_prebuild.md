---
content_title: prepare before build
---

## Ubuntu linux:
```sh
apt-get -y update
apt-get -y upgrade
apt-get -y install cmake
apt-get -y install clang
apt-get -y install git
apt-get -y install libdb-dev
apt-get -y install libdb++-dev
apt-get -y install libevent-dev

export GIT_SSL_NO_VERIFY=1
```