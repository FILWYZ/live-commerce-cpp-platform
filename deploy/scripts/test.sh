#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
build_dir="${project_dir}/build"
cmake -S "${project_dir}" -B "${build_dir}" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DENABLE_ASAN=OFF -DENABLE_UBSAN=OFF -DENABLE_TSAN=OFF
cmake --build "${build_dir}" --parallel
ctest --test-dir "${build_dir}" --output-on-failure

sanitizer_build_dir="${project_dir}/build-asan"
cmake -S "${project_dir}" -B "${sanitizer_build_dir}" -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_ASAN=ON -DENABLE_UBSAN=ON -DENABLE_TSAN=OFF
cmake --build "${sanitizer_build_dir}" --parallel
if [ "$(uname -s)" = "Darwin" ]; then
  ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}" UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1}" \
    ctest --test-dir "${sanitizer_build_dir}" --output-on-failure
else
  ctest --test-dir "${sanitizer_build_dir}" --output-on-failure
fi

if [ "${RUN_TSAN:-0}" = "1" ]; then
  tsan_build_dir="${project_dir}/build-tsan"
  cmake -S "${project_dir}" -B "${tsan_build_dir}" -DCMAKE_BUILD_TYPE=Debug \
    -DENABLE_ASAN=OFF -DENABLE_UBSAN=OFF -DENABLE_TSAN=ON
  cmake --build "${tsan_build_dir}" --parallel
  TSAN_OPTIONS="${TSAN_OPTIONS:-halt_on_error=1 exitcode=66}" \
    ctest --test-dir "${tsan_build_dir}" --output-on-failure
fi
