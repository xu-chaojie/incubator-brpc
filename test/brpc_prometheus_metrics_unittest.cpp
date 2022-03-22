// brpc - A framework to host and access services throughout Baidu.
// Copyright (c) 2018 BiliBili, Inc.
// Author: Jiashun Zhu(zhujiashun@bilibili.com)
// Date: Tue Dec 3 11:27:18 CST 2018

#include <gtest/gtest.h>
#include "brpc/server.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/strings/string_piece.h"
#include "echo.pb.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

class DummyEchoServiceImpl : public test::EchoService {
public:
    virtual ~DummyEchoServiceImpl() {}
    virtual void Echo(google::protobuf::RpcController* cntl_base,
                      const test::EchoRequest* request,
                      test::EchoResponse* response,
                      google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        return;
    }
};

enum STATE {
    HELP = 0,
    TYPE,
    GAUGE,
    SUMMARY
};

TEST(PrometheusMetrics, sanity) {
    brpc::Server server;
    DummyEchoServiceImpl echo_svc;
    ASSERT_EQ(0, server.AddService(&echo_svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start("127.0.0.1:8614", NULL));

    brpc::Channel channel;
    brpc::ChannelOptions channel_opts;
    channel_opts.protocol = "http";
    ASSERT_EQ(0, channel.Init("127.0.0.1:8614", &channel_opts));
    brpc::Controller cntl;
    cntl.http_request().uri() = "/metrics";
    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed());
    std::string res = cntl.response_attachment().to_string();

    size_t start_pos = 0;
    size_t end_pos = 0;
    STATE state = HELP;
    char name_help[128];
    char name_type[128];
    char type[16];
    int matched = 0;
    int gauge_num = 0;
    bool summary_sum_gathered = false;
    bool summary_count_gathered = false;
    bool has_ever_summary = false;
    bool has_ever_gauge = false;

    while ((end_pos = res.find('\n', start_pos)) != butil::StringPiece::npos) {
        res[end_pos] = '\0';       // safe;
        switch (state) {
            case HELP:
                matched = sscanf(res.data() + start_pos, "# HELP %s", name_help);
                ASSERT_EQ(1, matched);
                state = TYPE;
                break;
            case TYPE:
                matched = sscanf(res.data() + start_pos, "# TYPE %s %s", name_type, type);
                ASSERT_EQ(2, matched);
                ASSERT_STREQ(name_type, name_help);
                if (strcmp(type, "gauge") == 0) {
                    state = GAUGE;
                } else if (strcmp(type, "summary") == 0) {
                    state = SUMMARY;
                } else {
                    ASSERT_TRUE(false);
                }
                break;
            case GAUGE:
                matched = sscanf(res.data() + start_pos, "%s %d", name_type, &gauge_num);
                ASSERT_EQ(2, matched);
                ASSERT_STREQ(name_type, name_help);
                state = HELP;
                has_ever_gauge = true;
                break;
            case SUMMARY:
                if (butil::StringPiece(res.data() + start_pos, end_pos - start_pos).find("quantile=")
                        == butil::StringPiece::npos) {
                    matched = sscanf(res.data() + start_pos, "%s %d", name_type, &gauge_num);
                    ASSERT_EQ(2, matched);
                    ASSERT_TRUE(strncmp(name_type, name_help, strlen(name_help)) == 0);
                    if (butil::StringPiece(name_type).ends_with("_sum")) {
                        ASSERT_FALSE(summary_sum_gathered);
                        summary_sum_gathered = true;
                    } else if (butil::StringPiece(name_type).ends_with("_count")) {
                        ASSERT_FALSE(summary_count_gathered);
                        summary_count_gathered = true;
                    } else {
                        ASSERT_TRUE(false);
                    }
                    if (summary_sum_gathered && summary_count_gathered) {
                        state = HELP;
                        summary_sum_gathered = false;
                        summary_count_gathered = false;
                        has_ever_summary = true;
                    }
                } // else find "quantile=", just break to next line
                break;
            default:
                ASSERT_TRUE(false);
                break;
        }
        start_pos = end_pos + 1;
    }
    ASSERT_TRUE(has_ever_gauge && has_ever_summary);
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}
/*
TEST(PrometheusMetricsDumperTest, sanity) {
    butil::IOBufBuilder os;
    brpc::PrometheusMetricsDumper dumper(&os, "test");
    std::string test1("\"\"");
    ASSERT_FALSE(dumper.dump("test1", test1));

    std::string test2("\"hello word\"");
    ASSERT_FALSE(dumper.dump("test2", test2));

    std::string test3("\"{\"name\":\"haorooms\",\"address\":\"\"}\"");
    ASSERT_TRUE(dumper.dump("test3", test3));

    std::string test4("\"{\"name\":\"haorooms\",\"address\":\"word\"}\"");
    ASSERT_TRUE(dumper.dump("test4", test4));

    std::string test5("\"{\"name\":\"haorooms\",\"number\":1}\"");
    ASSERT_FALSE(dumper.dump("test5", test5));
    std::cout << "dump result:\n" << os << "\n";

    butil::IOBuf buf;
    os.move_to(buf);
    std::string strformat = buf.to_string();

    int end_pos = strformat.find('\n');
    ASSERT_TRUE(std::string::npos != end_pos);
    ASSERT_EQ("# HELP test3", strformat.substr(0, end_pos));
    end_pos++;
    strformat = strformat.substr(end_pos);

    end_pos = strformat.find('\n');
    ASSERT_TRUE(std::string::npos != end_pos);
    ASSERT_EQ("# TYPE test3 gauge", strformat.substr(0, end_pos));
    end_pos++;
    strformat = strformat.substr(end_pos);

    end_pos = strformat.find('\n');
    ASSERT_TRUE(std::string::npos != end_pos);
    ASSERT_EQ("test3{name=\"haorooms\",address=\"\"} 0",
              strformat.substr(0, end_pos));
    end_pos++;
    strformat = strformat.substr(end_pos);

    end_pos = strformat.find('\n');
    ASSERT_TRUE(std::string::npos != end_pos);
    ASSERT_EQ("# HELP test4", strformat.substr(0, end_pos));
    end_pos++;
    strformat = strformat.substr(end_pos);

    end_pos = strformat.find('\n');
    ASSERT_TRUE(std::string::npos != end_pos);
    ASSERT_EQ("# TYPE test4 gauge", strformat.substr(0, end_pos));
    end_pos++;
    strformat = strformat.substr(end_pos);

    ASSERT_EQ("test4{name=\"haorooms\",address=\"word\"} 0\n", strformat);

    // 运行结果
    // [==========] Running 1 test from 1 test case.
    // [----------] Global test environment set-up.
    // [----------] 1 test from PrometheusMetricsDumperTest
    // [ RUN      ] PrometheusMetricsDumperTest.sanity
    // dump result:
    // # HELP test3
    // # TYPE test3 gauge
    // test3{name="haorooms",address=""} 0
    // # HELP test4
    // # TYPE test4 gauge
    // test4{name="haorooms",address="word"} 0

    // [       OK ] PrometheusMetricsDumperTest.sanity (1 ms)
    // [----------] 1 test from PrometheusMetricsDumperTest (1 ms total)

    // [----------] Global test environment tear-down
    // [==========] 1 test from 1 test case ran. (1 ms total)
    // [  PASSED  ] 1 test.
}*/
