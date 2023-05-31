/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <sstream>
#include <random>
#include <chrono>
#include <thread>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_cat.h"

#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

ABSL_FLAG(std::string, target, "localhost:50051", "Server address");
ABSL_FLAG(int32_t, msg_size, 42, "Message request size");
ABSL_FLAG(std::string, dist_file, "", "Distribution file to send requests from");

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;
using helloworld::MessageSizeReply;
using helloworld::MessageSizeRequest;

class GreeterClient {
 public:
  GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string SayHello(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    // Container for the data we expect from the server.
    HelloReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->SayHello(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string GetMessageOfSize(const int32_t size) {
    // Data we are sending to the server.
    MessageSizeRequest request;
    request.set_size(size);

    // Container for the data we expect from the server.
    MessageSizeReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->GetMessageOfSize(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return absl::StrCat(reply.data().size(), " ", reply.data());
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<Greeter::Stub> stub_;
};

std::vector<std::pair<int, double>> read_dist_file(std::string fname) {
  std::vector<std::pair<int, double>> dist;
  
  std::ifstream fp(fname);
  if (fp.is_open()) {
    std::string line;
    int i = 0;
    while (std::getline(fp, line)) {
      if (i > 0) {
        std::istringstream ss(line);
        std::string split;
        int j = 0;
        int x;
        float y;
        while (std::getline(ss, split, ' ')) {
          if (j == 0) {
            x = std::stoi(split);
          } else {
            y = std::stof(split);
          }
          j += 1;
        }
        dist.push_back(std::make_pair(x, y));
        i += 1;
      }
      i += 1;
    }
  }
  return dist;
}

std::vector<int> sample_from_dist(std::vector<std::pair<int, double>> dist, int num_samples) {
  std::default_random_engine generator;
  std::uniform_real_distribution<double> distribution(0.0,1.0);

  std::vector<int> samples;

  while (num_samples-- > 0) {
    double val = distribution(generator);

    int lo = 0;
    int hi = dist.size() - 1;
    int curr = (lo + hi) / 2;
    auto p = dist[curr];
    while (hi - lo > 0) {
      // std::cout << lo << " " << curr << " " << hi << " " << p.first << " " << p.second << " " << val << std::endl;
      if (val <= p.second) {
        hi = curr;
      } else {
        lo = curr + 1;
      }
      curr = (lo + hi) / 2;
      p = dist[curr];
    }
    samples.push_back(p.first);
  }
  return samples;
}

std::vector<double> sample_sleep_times(int lambda, int num_samples) {
  std::default_random_engine generator;
  std::poisson_distribution<int> dist(lambda);

  std::vector<double> samples;
  while (num_samples-- > 0) {
    int sample = dist(generator);
    samples.push_back(1. / sample);
  }

  return samples;
}

void run_workload(std::string target_str, std::vector<int> flow_samples, std::vector<double> sleep_samples) {
  typedef std::chrono::high_resolution_clock myclock;

  GreeterClient greeter(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
  
  myclock::time_point start = myclock::now();

  uint64_t total_bytes = 0;
  for (auto i = 0; i < flow_samples.size(); i++) {
    total_bytes += flow_samples[i];

    greeter.GetMessageOfSize(flow_samples[i]);

    uint64_t sleepus = (int) (sleep_samples[i] * 1e6);
    std::this_thread::sleep_for(std::chrono::microseconds(sleepus));
  }
  myclock::time_point end = myclock::now();
  std::chrono::duration<double, std::micro> t_transfer = end - start;
  std::cout << total_bytes << " " << t_transfer.count() << " " << total_bytes * 8 * 1e6 / t_transfer.count() << std::endl;
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  std::string target_str = absl::GetFlag(FLAGS_target);
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).
  /*
  GreeterClient greeter(
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
  std::string user("world");
  std::string reply = greeter.SayHello(user);
  std::cout << "Greeter received: " << reply << std::endl;

  int32_t msg_size = absl::GetFlag(FLAGS_msg_size);
  std::string reply2 = greeter.GetMessageOfSize(msg_size);
  std::cout << "Greeter received: " << reply2 << std::endl;
  */

  std::string fname = absl::GetFlag(FLAGS_dist_file);
  std::cout << fname << std::endl;
  auto dist = read_dist_file(fname);

  int num_samples = 1000;
  auto flow_samples = sample_from_dist(dist, num_samples);
  auto sleep_samples = sample_sleep_times(1249, num_samples);

  run_workload(target_str, flow_samples, sleep_samples);

  return 0;
}
