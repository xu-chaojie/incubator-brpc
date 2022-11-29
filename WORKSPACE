workspace(name = "com_github_brpc_brpc")

load("//:bazel/workspace.bzl", "brpc_workspace")

brpc_workspace()

new_local_repository(
    name = "ucx",
    path = "/usr/local/ucx",
    build_file_content = """
package(default_visibility = ["//visibility:public"])
cc_library(
    name = "headers",
    hdrs = glob(["include/**/**/*.h"]),
    includes = ['include'],
)
"""
)

bind(
    name = "ucx_headers",
    actual = "@ucx//:headers",
)
