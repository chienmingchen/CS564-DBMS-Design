/**
 * @auther Hao-Lun Hsu (9081439458)
 *
 */

#pragma once

#include <string>

#include "badgerdb_exception.h"
#include "types.h"

namespace badgerdb {

/**
 * @brief An exception that is thrown when a page is not present in the buffer pool.
 */
class NotInBufferException : public BadgerDbException {
 public:
  /**
   * Constructs a not int buffer exception for the given page.
   */
  explicit NotInBufferException(const std::string& nameIn, PageId pageNoIn);

 protected:
  /**
   * Name of file that caused this exception.
   */
  const std::string& name;

  /**
   * Page number in file
   */
  const PageId pageNo;
};

}
