/**
 * @author Hao-Lun Hsu
 */

#pragma once

#include <string>

#include "badgerdb_exception.h"

namespace badgerdb {

/**
 * @brief An exception that is thrown when a non-leaf node is not full but is asked to be splitted
 */
class NonLeafNodeNotFullException : public BadgerDbException {
 public:
  /**
   * Constructs a non leaf node not full exception.
   */
  NonLeafNodeNotFullException();
};

}
