/* Copyright 2013-present Barefoot Networks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Antonin Bas (antonin@barefootnetworks.com)
 *
 */

//! @file pifo_tm.h
//! This file contains PIFO-based queueing logic. It is meant to serve
//! as a replacement for the typical QueueingLogic* classes.

#ifndef BM_BM_SIM_PIFO_QUEUEING_H_
#define BM_BM_SIM_PIFO_QUEUEING_H_

#include <deque>
#include <queue>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <algorithm>  // for std::max

namespace bm {

//! This is simple single PIFO-based queueing logic (i.e. not-hierarchical).
//! There are `nb_pifos` logical PIFOs and each one is mapped to a worker 
//! using the `FMap` template parameter (see below). Each PIFO stores the
//! enqueued elements in order of increasing priority (decreasing rank values).
//! The write behavior (enqueue()) is blocking: the function will not return
//! until it sucessfully enqueues the item.
//!
//! Template parameter `T` is the type (has to be movable) of the objects that
//! will be stored in the queues. Template parameter `FMap` is a callable object
//! that has to be able to map every logical queue id to a worker id. The
//! following is a good example of a functor that meets the requirements:
//! @code
//! struct WorkerMapper {
//!   WorkerMapper(size_t nb_workers)
//!       : nb_workers(nb_workers) { }
//!
//!   size_t operator()(size_t pifo_id) const {
//!     return pifo_id % nb_workers;
//!   }
//!
//!   size_t nb_workers;
//! };
//! @endcode
template <typename T, typename FMap>
class PifoQueueingLogic {
 public:
  //! \p nb_pifos is the number of logical PIFOs; each PIFO is identified by
  //! an id in the range `[0, nb_pifos)` when performing an enqueue. \p
  //! nb_workers is the number of threads that will be consuming from the
  //! PIFOs; they will be identified by an id in the range `[0,
  //! nb_workers)`. \p capacity is the number of objects that each PIFO
  //! can hold. Because we need to be able to map each PIFO id to a worker id,
  //! the user has to provide a callable object of type `FMap`, \p
  //! map_to_worker, that can do this mapping. See the PifoQueueingLogic class
  //! description for more information about the `FMap` template parameter.
  PifoQueueingLogic(size_t nb_pifos, size_t nb_workers, size_t capacity,
                FMap map_to_worker)
      : nb_pifos(nb_pifos), nb_workers(nb_workers),
        pifos_info(nb_pifos), workers_info(nb_workers),
        map_to_worker(std::move(map_to_worker)) {
    for (auto &p_info : pifos_info)
      p_info.capacity = capacity;
  }

  //! Makes a copy of \p item and inserts it into the PIFO with id \p pifo_id
  //! using priority \p rank. 
  void enqueue(size_t pifo_id, size_t rank, const T &item) {
    size_t worker_id = map_to_worker(pifo_id);
    auto &p_info = pifos_info.at(pifo_id);
    auto &w_info = workers_info.at(worker_id);
    std::unique_lock<std::mutex> lock(w_info.p_mutex);
    while (p_info.size >= p_info.capacity) {
      p_info.p_not_full.wait(lock);
    }
    w_info.pifo.emplace(item, rank, pifo_id);
    p_info.size++;
    w_info.p_not_empty.notify_one();
  }

  //! Moves \p item into the logical PIFO with id \p pifo_id using priority
  //! \p rank.
  void enqueue(size_t pifo_id, size_t rank, T &&item) {
    size_t worker_id = map_to_worker(pifo_id);
    auto &p_info = pifos_info.at(pifo_id);
    auto &w_info = workers_info.at(worker_id);
    std::unique_lock<std::mutex> lock(w_info.p_mutex);
    while (p_info.size >= p_info.capacity) {
      p_info.p_not_full.wait(lock);
    }
    w_info.pifo.emplace(std::move(item), rank, pifo_id);
    p_info.size++;
    w_info.p_not_empty.notify_one();
  }

  //! Retrieves the highest priority element for the worker thread indentified by \p
  //! worker_id and moves it to \p pItem. The id of the logical PIFO which
  //! contained this element is copied to \p pifo_id and the priority of the element
  //! is copied to \p rank.
  //! As a reminder, the `map_to_worker` argument provided when constructing the
  //! class is used to map every pifo id to the corresponding worker id. Therefore,
  //! if an element `E` was pushed to PIFO `pifo_id`, you need to use the worker id
  //! `map_to_worker(pifo_id)` to retrieve it with this function.
  void dequeue(size_t worker_id, size_t *pifo_id, size_t *rank, T *pItem) {
    auto &w_info = workers_info.at(worker_id);
    auto &pifo = w_info.pifo;
    std::unique_lock<std::mutex> lock(w_info.p_mutex);
    while (pifo.size() == 0) {
      w_info.p_not_empty.wait(lock);
    }
    *rank = pifo.top().rank;
    *pifo_id = pifo.top().pifo_id;
    *pItem = std::move(pifo.top().e);
    pifo.pop();
    auto &p_info = pifos_info.at(*pifo_id);
    p_info.size--;
    p_info.p_not_full.notify_one();
  }

  //! Get the occupancy of the logical PIFO with id \p pifo_id.
  size_t size(size_t pifo_id) const {
    size_t worker_id = map_to_worker(pifo_id);
    auto &p_info = pifos_info.at(pifo_id);
    auto &w_info = workers_info.at(worker_id);
    std::unique_lock<std::mutex> lock(w_info.p_mutex);
    return p_info.size;
  }

  //! Set the capacity of the logical PIFO with id \p pifo_id to \p c
  //! elements.
  void set_capacity(size_t pifo_id, size_t c) {
    size_t worker_id = map_to_worker(pifo_id);
    auto &p_info = pifos_info.at(pifo_id);
    auto &w_info = workers_info.at(worker_id);
    std::unique_lock<std::mutex> lock(w_info.p_mutex);
    p_info.capacity = c;
  }

  //! Deleted copy constructor
  PifoQueueingLogic(const PifoQueueingLogic &) = delete;
  //! Deleted copy assignment operator
  PifoQueueingLogic &operator =(const PifoQueueingLogic &) = delete;

  //! Deleted move constructor
  PifoQueueingLogic(PifoQueueingLogic &&) = delete;
  //! Deleted move assignment operator
  PifoQueueingLogic &&operator =(PifoQueueingLogic &&) = delete;

 private:
  //! A PIFO element
  struct PE {
    PE(T e, size_t rank, size_t pifo_id)
        : e(std::move(e)), rank(rank), pifo_id(pifo_id) { }

    T e;
    size_t rank;
    size_t pifo_id;
  };

  struct PEComp {
    bool operator()(const PE &lhs, const PE &rhs) const {
      return lhs.rank < rhs.rank;
    }
  };

  using MyPIFO = std::priority_queue<PE, std::deque<PE>, PEComp>;

  struct PifoInfo {
    size_t size{0};
    size_t capacity{0};
    mutable std::condition_variable p_not_full{};
  };

  struct WorkerInfo {
    MyPIFO pifo{};
    mutable std::mutex p_mutex{};
    mutable std::condition_variable p_not_empty{};
  };

  size_t nb_pifos;
  size_t nb_workers;
  std::vector<PifoInfo> pifos_info;
  std::vector<WorkerInfo> workers_info;
  FMap map_to_worker;
};

}  // namespace bm

#endif  // BM_BM_SIM_PIFO_QUEUEING_H_
