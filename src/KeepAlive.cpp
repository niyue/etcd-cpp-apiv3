#include <chrono>
#include <cstdlib>
#include <iostream>
#include <ratio>

#include "etcd/KeepAlive.hpp"
#include "etcd/v3/AsyncGRPC.hpp"

#include <grpc++/grpc++.h>
#include "proto/rpc.grpc.pb.h"

namespace etcdv3 {
class AsyncLeaseKeepAliveAction;
}

namespace {
// Calculate keepalive heartbeat interval based on TTL
// Can be overridden by ETCD_KEEPALIVE_INTERVAL environment variable
// Format: "ETCD_KEEPALIVE_INTERVAL=<seconds>" for fixed interval
//         "ETCD_KEEPALIVE_RATIO=<percent>" for ratio-based (e.g., 33 for 1/3)
// Default: 1/3 of TTL if not set
int calculate_keepalive_ttl(int ttl) {
  const char* interval_env = std::getenv("ETCD_KEEPALIVE_INTERVAL");
  if (interval_env != nullptr) {
    try {
      int interval = std::stoi(interval_env);
      return std::max(interval, 1);
    } catch (...) {
      std::cerr << "[warn] Invalid ETCD_KEEPALIVE_INTERVAL value: " << interval_env
                << ", using default (1/3 TTL)" << std::endl;
    }
  }

  const char* ratio_env = std::getenv("ETCD_KEEPALIVE_RATIO");
  if (ratio_env != nullptr) {
    try {
      int ratio = std::stoi(ratio_env);
      if (ratio > 0 && ratio <= 100) {
        return std::max((ttl * ratio) / 100, 1);
      }
      std::cerr << "[warn] Invalid ETCD_KEEPALIVE_RATIO value: " << ratio_env
                << " (must be 1-100), using default (1/3 TTL)" << std::endl;
    } catch (...) {
      std::cerr << "[warn] Invalid ETCD_KEEPALIVE_RATIO value: " << ratio_env
                << ", using default (1/3 TTL)" << std::endl;
    }
  }

  // Default: 1/3 of TTL
  return std::max(ttl / 3, 1);
}
}  // namespace

struct etcd::KeepAlive::EtcdServerStubs {
  std::unique_ptr<etcdserverpb::Lease::Stub> leaseServiceStub;
  std::unique_ptr<etcdv3::AsyncLeaseKeepAliveAction> call;
};

void etcd::KeepAlive::EtcdServerStubsDeleter::operator()(
    etcd::KeepAlive::EtcdServerStubs* stubs) {
  if (stubs) {
    delete stubs;
  }
}

etcd::KeepAlive::KeepAlive(SyncClient const& client, int ttl, int64_t lease_id)
    : ttl(ttl),
      lease_id(lease_id),
      continue_next(true),
      grpc_timeout(client.get_grpc_timeout()) {
  if (ttl > 0 && lease_id == 0) {
    this->lease_id =
        const_cast<SyncClient&>(client).leasegrant(ttl).value().lease();
  }
  stubs.reset(new EtcdServerStubs{});
  stubs->leaseServiceStub = Lease::NewStub(client.grpc_channel());

  etcdv3::ActionParameters params;
  params.auth_token.assign(client.current_auth_token());
  params.grpc_timeout = grpc_timeout;
  params.lease_id = this->lease_id;
  params.lease_stub = stubs->leaseServiceStub.get();

  continue_next.store(true);

  stubs->call.reset(new etcdv3::AsyncLeaseKeepAliveAction(std::move(params)));
  refresh_task_ = std::thread([this]() {
#ifndef _ETCD_NO_EXCEPTIONS
    try {
      // start refresh
      this->refresh();
    } catch (const std::exception& e) {
      // propagate the exception
      eptr_ = std::current_exception();
    }
#else
    const std::string err = this->refresh();
    if (!err.empty()) {
      eptr_ = std::make_exception_ptr(std::runtime_error(err));
    }
#endif
  });
}

etcd::KeepAlive::KeepAlive(std::string const& address, int ttl,
                           int64_t lease_id)
    : KeepAlive(SyncClient(address), ttl, lease_id) {}

etcd::KeepAlive::KeepAlive(std::string const& address,
                           std::string const& username,
                           std::string const& password, int ttl,
                           int64_t lease_id, int const auth_token_ttl)
    : KeepAlive(SyncClient(address, username, password, auth_token_ttl), ttl,
                lease_id) {}

etcd::KeepAlive::KeepAlive(
    std::string const& address, std::string const& ca, std::string const& cert,
    std::string const& privkey,
    std::function<void(std::exception_ptr)> const& handler, int ttl,
    int64_t lease_id, std::string const& target_name_override)
    : KeepAlive(SyncClient(address, ca, cert, privkey, target_name_override),
                handler, ttl, lease_id) {}

etcd::KeepAlive::KeepAlive(
    SyncClient const& client,
    std::function<void(std::exception_ptr)> const& handler, int ttl,
    int64_t lease_id)
    : handler_(handler),
      ttl(ttl),
      lease_id(lease_id),
      continue_next(true),
      grpc_timeout(client.get_grpc_timeout()) {
  if (ttl > 0 && lease_id == 0) {
    this->lease_id =
        const_cast<SyncClient&>(client).leasegrant(ttl).value().lease();
  }
  stubs.reset(new EtcdServerStubs{});
  stubs->leaseServiceStub = Lease::NewStub(client.grpc_channel());

  etcdv3::ActionParameters params;
  params.auth_token.assign(client.current_auth_token());
  // n.b.: keepalive: no need for timeout
  params.lease_id = this->lease_id;
  params.lease_stub = stubs->leaseServiceStub.get();

  stubs->call.reset(new etcdv3::AsyncLeaseKeepAliveAction(std::move(params)));
  refresh_task_ = std::thread([this]() {
#ifndef _ETCD_NO_EXCEPTIONS
    try {
      // start refresh
      this->refresh();
    } catch (...) {
      // propagate the exception
      eptr_ = std::current_exception();
    }
#else
    const std::string err = this->refresh();
    if (!err.empty()) {
      eptr_ = std::make_exception_ptr(std::runtime_error(err));
    }
#endif
    if (eptr_ && handler_) {
      handler_(eptr_);
    }
  });
}

etcd::KeepAlive::KeepAlive(
    std::string const& address,
    std::function<void(std::exception_ptr)> const& handler, int ttl,
    int64_t lease_id)
    : KeepAlive(SyncClient(address), handler, ttl, lease_id) {}

etcd::KeepAlive::KeepAlive(
    std::string const& address, std::string const& username,
    std::string const& password,
    std::function<void(std::exception_ptr)> const& handler, int ttl,
    int64_t lease_id, const int auth_token_ttl)
    : KeepAlive(SyncClient(address, username, password, auth_token_ttl),
                handler, ttl, lease_id) {}

etcd::KeepAlive::~KeepAlive() { this->Cancel(); }

void etcd::KeepAlive::Cancel() {
  if (!continue_next.exchange(false)) {
    return;
  }

  // stop the thread
  cv_for_refresh_.notify_all();
  refresh_task_.join();

  // send a cancel request
  {
    std::lock_guard<std::mutex> lock(mutex_for_refresh_);
    stubs->call->CancelKeepAlive();
  }
}

void etcd::KeepAlive::Check() {
  if (eptr_) {
    std::rethrow_exception(eptr_);
  }
  // issue an refresh to make sure it still alive
#ifndef _ETCD_NO_EXCEPTIONS
  try {
    this->refresh_once();
  } catch (...) {
    // run canceller first
    this->Cancel();

    // propagate the exception, as we throw in `Check()`, the `handler` won't be
    // touched
    eptr_ = std::current_exception();
  }
#else
  const std::string err = this->refresh_once();
  if (!err.empty()) {
    // run canceller first
    this->Cancel();

    // propagate the exception, as we throw in `Check()`, the `handler` won't be
    // touched
    eptr_ = std::make_exception_ptr(std::runtime_error(err));
  }
#endif
  if (eptr_ && handler_) {
    handler_(eptr_);
  }

#ifndef _ETCD_NO_EXCEPTIONS
  if (eptr_) {
    // rethrow in `Check()` to keep the consistent semantics
    std::rethrow_exception(eptr_);
  }
#endif
  return;
}

std::string etcd::KeepAlive::refresh() {
  while (true) {
    if (!continue_next.load()) {
      return std::string{};
    }
    // Calculate heartbeat interval as 1/3 of TTL to allow for 2 missed heartbeats
    // before lease expiration, providing sufficient buffer for network latency and clock drift
    // Can be overridden by ETCD_KEEPALIVE_INTERVAL or ETCD_KEEPALIVE_RATIO environment variables
    int keepalive_ttl = calculate_keepalive_ttl(ttl);
    {
      std::unique_lock<std::mutex> lock(mutex_for_refresh_);
      if (cv_for_refresh_.wait_for(lock, std::chrono::seconds(keepalive_ttl)) ==
          std::cv_status::no_timeout) {
        if (!continue_next.load()) {
          return std::string{};
        }
#ifndef NDEBUG
        std::cerr
            << "[warn] awaked from condition_variable but continue_next is "
               "not set, maybe due to clock drift."
            << std::endl;
#endif
      }
    }

    // execute refresh
    const std::string err = this->refresh_once();
    if (!err.empty()) {
      return err;
    }
  }
  return std::string{};
}

std::string etcd::KeepAlive::refresh_once() {
  std::lock_guard<std::mutex> scope_lock(mutex_for_refresh_);
  if (!continue_next.load()) {
    return std::string{};
  }
  this->stubs->call->mutable_parameters().grpc_timeout = this->grpc_timeout;
  auto resp = this->stubs->call->Refresh();
  if (!resp.is_ok()) {
    const std::string err = "Failed to refresh lease: error code: " +
                            std::to_string(resp.error_code()) +
                            ", message: " + resp.error_message();
#ifndef _ETCD_NO_EXCEPTIONS
    throw std::runtime_error(err);
#endif
    return err;
  }
  if (resp.value().ttl() == 0) {
    const std::string err =
        "Failed to refresh lease due to expiration: the new TTL is 0.";
#ifndef _ETCD_NO_EXCEPTIONS
    throw std::out_of_range(err);
#endif
    return err;
  }
  return std::string{};
}
