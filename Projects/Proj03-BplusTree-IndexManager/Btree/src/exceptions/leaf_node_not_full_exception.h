/**
 * @author Hao-Lun Hsu
 */

#pragma once

#include <string>

#include "badgerdb_exception.h"

namespace badgerdb {

/**
 * @brief An exception that is thrown when a leaf node is not full but is asked to be splitted
 */
class LeafNodeNotFullException : public BadgerDbException {
 public:
  /**
   * Constructs a leaf node not full exception.
   */
  LeafNodeNotFullException();
};

}
