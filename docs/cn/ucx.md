# RDMA网络通讯的使用

简介
===

网易为BRPC自主实现了RDMA通讯，网易的RDMA实现是基于开源项目UCX，关于UCX的详细情况可以参见
<a>https://github.com/openucx/ucx</a>

对BRPC的修改主要在BRPC的Socket的层面展开，引入了UCX中的UCP连接，UCP通讯支持
Active message、Tag 和Stream，我们的实现使用Active Message，并允许busy poll和
乱序消息投递，以提高性能。这些实现对于上层API都是透明的。


概念
===

BRPC中的通讯主要集中在以下几个基本概念：

1. EndPoint

  UCP通讯的地址形式与TCP相同，都是IP地址和PORT。但是它们在底层走不同的协议。
  EndPoint是一个代表通讯地址的数据结构, 是一个C++类，由于现在要支持不同的通讯端口类型，我们增加了枚举字段kind, 其值由TCP、UCP和UNIX组成，并增加了一些构造函数以接受kind值。
  同时，一些辅助函数例如set_ucp(), set_tcp()可以直接改变类型。在引入unix socket类型之前本来
  这个修改是二进制兼容原始的BRPC的，原来port是32位，我们将port变成short，并把kind也设计成short，这样原来一个int的大小依然得以保留，不过现在引入了unix socket path, 这个二进制格式不再兼容原始BRPC。


  ```
  struct EndPoint {
    enum { TCP, UCP, UNIX };
    EndPoint() : ip(IP_ANY), port(0), kind(TCP), socket_file("") {}
    EndPoint(ip_t ip2, int port2) : ip(ip2), port(port2), kind(TCP), socket_file("") {}
    EndPoint(ip_t ip2, int port2, int k) : ip(ip2), port(port2), kind(k), socket_file("") {}
    explicit EndPoint(const sockaddr_in& in)
        : ip(in.sin_addr), port(ntohs(in.sin_port)), kind(TCP), socket_file("") {}
    explicit EndPoint(const sockaddr_in& in, int k)
        : ip(in.sin_addr), port(ntohs(in.sin_port)), kind(k), socket_file("") {}
    explicit EndPoint(const char* file) : ip(IP_ANY), port(0), kind(UNIX),
                                            socket_file(file) {}
    explicit EndPoint(const std::string& file) : ip(IP_ANY), port(0), kind(UNIX),
                                            socket_file(file) {}

    bool is_tcp() const { return TCP == kind; }
    void set_tcp() { kind = TCP; }
    bool is_ucp() const { return UCP == kind; }
    void set_ucp() { kind = UCP; }
    bool is_unix() const { return UNIX == kind; }
    void set_unix() { kind = UNIX; }

    ip_t ip;
#if ARCH_CPU_LITTLE_ENDIAN
    unsigned short port;
    unsigned short kind;
#else
    unsigned short kind;
    unsigned short port;
#endif
    std::string socket_file;
};

```

2. ChannelOptions

  当BRPC的client与server连接时，需要一个Channel对象，而通常初始化这个对象又涉及不少参数，于是BRPC用了ChannelOptions来封装这些参数。我们对UCP RDMA网络通讯的支持就是通过修改ChannelOptions, 增加字段use_ucp，当这个字段被设置为true时，BRPC socket层将使用UCP连接大致的ChannelOptions如下所示：


```
  struct ChannelOptions {
    // Constructed with default options.
    ChannelOptions();

    // Issue error when a connection is not established after so many
    // milliseconds. -1 means wait indefinitely.
    // Default: 200 (milliseconds)
    // Maximum: 0x7fffffff (roughly 30 days)
    int32_t connect_timeout_ms;

    // Max duration of RPC over this Channel. -1 means wait indefinitely.
    // Overridable by Controller.set_timeout_ms().
    // Default: 500 (milliseconds)
    // Maximum: 0x7fffffff (roughly 30 days)
    int32_t timeout_ms;

    // Send another request if RPC does not finish after so many milliseconds.
    // Overridable by Controller.set_backup_request_ms().
    // The request will be sent to a different server by best effort.
    // If timeout_ms is set and backup_request_ms >= timeout_ms, backup request
    // will never be sent.
    // backup request does NOT imply server-side cancelation.
    // Default: -1 (disabled)
    // Maximum: 0x7fffffff (roughly 30 days)
    int32_t backup_request_ms;

    // Retry limit for RPC over this Channel. <=0 means no retry.
    // Overridable by Controller.set_max_retry().
    // Default: 3
    // Maximum: INT_MAX
    int max_retry;

 ......

    // Use ucp transport
    bool use_ucp;


};

```

以上所见，我们只增加了use_ucp一项。

3. Channel

Channel是Client实际与Server发生连接的对象，
```
class Channel : public ChannelBase {                                            
friend class Controller;                                                        
friend class SelectiveChannel;                                                  
public:                                                                         
    Channel(ProfilerLinker = ProfilerLinker());                                 
    ~Channel();
    // Connect this channel to a single server whose address is given by the    
    // first parameter. Use default options if `options' is NULL.               
    int Init(butil::EndPoint server_addr_and_port, const ChannelOptions* options);
    int Init(const char* server_addr_and_port, const ChannelOptions* options);  
    int Init(const char* server_addr, int port, const ChannelOptions* options);
    int InitWithSockFile(const char* socket_file, const ChannelOptions* options);
......
}

```

我们在上面第一个Init函数里进行了检查，server_addr_and_port里的kind字段如果是UCP，必须与
Options中的use_tcp保持一致。


4. ServerOptions

BRCP的服务器使用了Server类，这个类我们没有改动，而是对brpc::ServerOptions options增加了字段以支持UCP的RDMA连接。


```
struct ServerOptions {                                                          
    ServerOptions();  // Constructed with default options.                      

.....

    // Enable ucp listener                                                      
    bool enable_ucp;                                                            

    // Ucp listener ip address                                                  
    std::string ucp_address;                                                    

    // Ucp listener port                                                        
    uint16_t ucp_port;          
}

```

enable_ucp这个字段如果为true，则ucp_address和ucp_port将被使用来打开UCP侦听端口。

5. Server

Server类具有Start函数，这个函数始终接受一个TCP地址，这些参数是为了打开TCP服务，在我们的修改里,tcp连接永远被使能，这和老的client兼容，不强制唯一地使用UCP RDMA网络通讯。UCP RDMA的使能总是在ServerOptions中被指定。

```
class Server {
public:
    int Start(const char* ip_port_str, const ServerOptions* opt);               
    int Start(const butil::EndPoint& ip_port, const ServerOptions* opt);        
    // Start on IP_ANY:port.                                                    
    int Start(int port, const ServerOptions* opt);                              
    // Start on `ip_str' + any useable port in `range'                          
    int Start(const char* ip_str, PortRange range, const ServerOptions *opt);
    int Start(const char* ip_port_str, const ServerOptions* opt);
}

```

获取与安装
========

1. UCX的安装

```
  mkdir ~/github
  cd github
  git clone git@github.com:openucx/ucx.git
  cd ucx
  ./autogen.sh
  ./configure --prefix=/usr/local/ucx
  make
  sudo make install
```

2. BRPC的编译

```
  cd ~/github
  git clone git@github.com:opencurve/incubator-brpc.git
  cd incubator-brpc
  git checkout ucx_am
  mkdir bu
  cd bu
  cmake ..
  make
  cd ../example/multi_threaded_echo_c++
  mkdir bu
  cd bu
  cmake ..
  make
  cp ../*.pem .
```

实际使用例子
==========

1. 修改

我们对BRPC的multi_thread_echo_c++这个例子进行修改，使得它支持UCP RDMA网络。
<a>https://github.com/opencurve/incubator-brpc/tree/ucx_am/example/multi_threaded_echo_c++</a>

对client修改主要是用命令行参数设置Options中的use_tcp:

```
    options.use_ucp = FLAGS_use_ucp;
    if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }
```

 --use_ucp=true这个命令行参数启用UCP。

对server的修改主要是:
```
options.enable_ucp = FLAGS_enable_ucp;
options.ucp_address = FLAGS_ucp_address;
options.ucp_port = FLAGS_ucp_port;
if (server.Start(FLAGS_port, &options) != 0) {
    LOG(ERROR) << "Fail to start EchoServer";
    return -1;
}
```
以下命令行参数启用UCP：
  --enable_ucp=true --ucp_address=127.0.0.1 --ucp_port=13339


2. 运行时配置

ucx需要被配置以适合我们使用，例如

$HOME/ucx.conf
具有以下内容:

```
[global]                                                                        
UCX_TCP_CM_REUSEADDR=y
UCX_ASYNC_MAX_EVENTS=1000000
UCX_RDMA_CM_REUSEADDR=y
UCX_RNDV_THRESH=128K
UCX_TCP_TX_SEG_SIZE=32K
UCX_TCP_MAX_IOV=128
UCX_TLS=^tcp
UCX_USE_MT_MUTEX=y
```

3. 运行

- 服务器端:

```
./multi_threaded_echo_server --brpc_ucp_worker_busy_poll=1
I0811 11:29:09.381323 551185 /home/incubator-brpc/src/brpc/ucp_ctx.cpp:35] Running with ucp library version: 1.14.0
I0811 11:29:09.410902 551185 /home/incubator-brpc/src/brpc/ucp_acceptor.cpp:321] Ucp server is listening on IP 0.0.0.0 port 13339
I0811 11:29:09.410927 551185 /home/incubator-brpc/src/brpc/server.cpp:1133] Server[example::EchoServiceImpl] is serving on port=8002.
I0811 11:29:09.411169 551185 /home/incubator-brpc/src/brpc/server.cpp:1136] Check out xxx in web browser.
I0811 11:30:17.837671 551233 /home/incubator-brpc/src/brpc/ucp_acceptor.cpp:399] UCP server received a connection request from client at address 10.187.0.6:53216
E0811 11:30:22.988961 551248 /home/incubator-brpc/src/brpc/ucp_worker.cpp:732] Error occurred on remote side 10.187.0.6:53216 (Connection reset by remote peer)
I0811 11:30:39.837903 551244 /home/incubator-brpc/src/brpc/ucp_acceptor.cpp:399] UCP server received a connection request from client at address 10.187.0.6:55760
E0811 11:30:53.501705 551248 /home/incubator-brpc/src/brpc/ucp_worker.cpp:732] Error occurred on remote side 10.187.0.6:55760 (Connection reset by remote peer)
```

- 客户端  

```
UCX_TLS=^tcp ./multi_threaded_echo_client --server=10.187.0.91:13339 --use_ucp=true --thread_num=1 --brpc_ucp_worker_busy_poll=1 --attachment_size=4096
I0811 11:30:39.776002 963962 /home/incubator-brpc/src/brpc/ucp_ctx.cpp:35] Running with ucp library version: 1.14.0
I0811 11:30:40.775541 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=19186 latency=50
I0811 11:30:41.775643 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20026 latency=47
I0811 11:30:42.775714 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=18512 latency=50
I0811 11:30:43.775791 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=18512 latency=50
I0811 11:30:44.775864 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=18406 latency=51
I0811 11:30:45.775959 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20643 latency=46
I0811 11:30:46.776042 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20873 latency=46
I0811 11:30:47.776130 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20907 latency=46
I0811 11:30:48.776197 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20901 latency=46
I0811 11:30:49.776281 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20894 latency=46
I0811 11:30:50.776350 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20967 latency=46
I0811 11:30:51.776426 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20923 latency=46
I0811 11:30:52.776510 963950 /home/incubator-brpc/example/multi_threaded_echo_c++/client.cpp:144] Sending EchoRequest at qps=20923 latency=46


```

总结
===

从上面的例子可以看出，使用RDMA并不复杂，我们通过简要增加一些字段完成了对RDMA接口的支持，具有
良好的API兼容性，也获得了RDMA出色的性能。
