## Dependencies

### Rocky 8.9/9.3

```sh
sudo dnf install -y epel-release
sudo dnf install -y wget tar git gcc-toolset-13* perl flex bison patch fuse3-devel  libnl3-devel libunwind-devel python3-devel

wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz
tar zxvf cmake-3.30.1-linux-x86_64.tar.gz
sudo cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/ &&   sudo cp -rf  cmake-3.30.1-linux-x86_64/share/* /usr/local/share && rm -rf cmake-3.30.1-linux-x86_64

source /opt/rh/gcc-toolset-13/enable
```

### Ubuntu 22.04/24.04

```sh
sudo apt update
sudo apt install -y wget tar git make patch gcc g++ perl flex bison libnl-genl-3-dev libunwind-dev libfuse3-dev python3-dev

wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz
tar zxvf cmake-3.30.1-linux-x86_64.tar.gz
sudo cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/ && sudo cp -rf  cmake-3.30.1-linux-x86_64/share/* /usr/local/share && rm -rf cmake-3.30.1-linux-x86_64
```

## Compilation

###  Install bazel

```sh
wget https://github.com/bazelbuild/bazel/releases/download/4.2.4/bazel-4.2.4-linux-x86_64
sudo mv bazel-4.2.4-linux-x86_64 /usr/local/bin/bazel
sudo chmod a+x /usr/local/bin/bazel
```

### Build Third Party

```sh
git submodule sync
git submodule update --init --recursive

cd third-party
cmake -S . -B build
cmake --build build -j 16
```

### Build Etcd Client

```sh
bash build_thirdparties.sh
```

### Build Curve and CurveFS
```sh
export BAZEL_JOBS=16

bash build.sh

bash buildfs.sh
```